//! Encoders for `__barrier_state` records, for the crate's own unit tests.
//!
//! The broker writes these records, never a client, so the encoders live under
//! `cfg(test)` instead of on the public surface. They mirror the frozen layouts
//! that [`super::cut`] documents.

use std::collections::{BTreeMap, BTreeSet};

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    barrier::cut::{BarrierCut, CutStatus},
    membership::TopicPartition,
    runtime::iqv2::request::Position,
};

/// One cut record, as the fields a test wants to name.
#[derive(Debug, Clone)]
pub(crate) struct CutRecord {
    pub group: String,
    pub epoch: i64,
    pub triggered_at: i64,
    pub completed_at: i64,
    pub status: CutStatus,
    /// `(topic, partition, marker offset)` triples.
    pub offsets: Vec<(&'static str, i32, i64)>,
    /// `(topic, partition)` pairs that received no marker.
    pub missing: Vec<(&'static str, i32)>,
}

impl CutRecord {
    /// A complete cut with no missing partitions and fixed timestamps.
    pub(crate) fn simple(group: &str, epoch: i64, offsets: &[(&'static str, i32, i64)]) -> Self {
        Self {
            group: group.to_owned(),
            epoch,
            triggered_at: 1,
            completed_at: 2,
            status: CutStatus::Complete,
            offsets: offsets.to_vec(),
            missing: Vec::new(),
        }
    }

    /// The same cut with a partial status and the named missing partitions.
    pub(crate) fn partial(mut self, missing: &[(&'static str, i32)]) -> Self {
        self.status = CutStatus::Partial;
        self.missing = missing.to_vec();
        self
    }

    /// The cut a decoder must produce for these bytes.
    pub(crate) fn expected(&self) -> BarrierCut {
        let mut offsets: BTreeMap<String, BTreeMap<i32, i64>> = BTreeMap::new();
        for (topic, partition, offset) in &self.offsets {
            offsets
                .entry((*topic).to_owned())
                .or_default()
                .insert(*partition, *offset);
        }
        BarrierCut {
            group: self.group.clone(),
            epoch: self.epoch,
            triggered_at: self.triggered_at,
            completed_at: self.completed_at,
            status: self.status,
            offsets: Position(offsets),
            missing: self
                .missing
                .iter()
                .map(|(topic, partition)| TopicPartition {
                    topic: (*topic).to_owned(),
                    partition: *partition,
                })
                .collect::<BTreeSet<_>>(),
        }
    }
}

fn put_string(buffer: &mut BytesMut, value: &str) {
    buffer.put_i16(i16::try_from(value.len()).expect("string length fits i16"));
    buffer.extend_from_slice(value.as_bytes());
}

/// The key of a record of any kind.
pub(crate) fn key_of_kind(kind: i16, group: &str, epoch: i64) -> Bytes {
    let mut buffer = BytesMut::new();
    buffer.put_i16(0); // version
    buffer.put_i16(kind);
    put_string(&mut buffer, group);
    buffer.put_i64(epoch);
    buffer.freeze()
}

/// The key of a cut record.
pub(crate) fn cut_key(record: &CutRecord) -> Bytes {
    key_of_kind(2, &record.group, record.epoch)
}

/// The value of a cut record.
pub(crate) fn cut_value(record: &CutRecord) -> Bytes {
    let mut by_topic: BTreeMap<&str, Vec<(i32, i64)>> = BTreeMap::new();
    for (topic, partition, offset) in &record.offsets {
        by_topic
            .entry(topic)
            .or_default()
            .push((*partition, *offset));
    }
    let mut buffer = BytesMut::new();
    buffer.put_i16(0); // version
    buffer.put_i64(record.triggered_at);
    buffer.put_i64(record.completed_at);
    buffer.put_i8(record.status.code());
    buffer.put_i32(i32::try_from(by_topic.len()).expect("topic count fits i32"));
    for (topic, partitions) in &by_topic {
        put_string(&mut buffer, topic);
        buffer.put_i32(i32::try_from(partitions.len()).expect("partition count fits i32"));
        for (partition, offset) in partitions {
            buffer.put_i32(*partition);
            buffer.put_i64(*offset);
        }
    }
    buffer.put_i32(i32::try_from(record.missing.len()).expect("missing count fits i32"));
    for (topic, partition) in &record.missing {
        put_string(&mut buffer, topic);
        buffer.put_i32(*partition);
    }
    buffer.freeze()
}
