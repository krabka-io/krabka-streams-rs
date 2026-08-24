//! The `__barrier_state` record layouts and the decoded cut.
//!
//! A krabka broker injects an epoch-stamped marker into every partition of a
//! barrier group and then publishes the resulting cut to the internal topic
//! [`BARRIER_STATE_TOPIC`]. The topic carries three record kinds, and the key
//! discriminates them. Only the cut kind matters to a client, so
//! [`decode_barrier_cut`] returns `None` for a group definition, for an
//! injection start, and for a tombstone.
//!
//! The layouts below are frozen. The broker, `krabka-streams-rs`,
//! `krabka-streams-java`, and `krabka-streams-go` all write and read them:
//!
//! ```text
//! key:
//!   version  i16 = 0
//!   kind     i16               0 group, 1 injection start, 2 cut
//!   group    string
//!   epoch    i64               -1 for kind 0
//!
//! cut value:
//!   version      i16 = 0
//!   triggered_at i64
//!   completed_at i64
//!   status       i8            0 complete, 1 partial
//!   topics       i32 [ topic string | partitions i32 [ partition i32 | offset i64 ] ]
//!   missing      i32 [ topic string | partition i32 ]
//! ```
//!
//! A `string` is an `i16` byte length and then UTF-8 bytes. Every integer is
//! big-endian, and an `i32` count precedes each array.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    error::StreamsClientError, membership::TopicPartition, runtime::iqv2::request::Position,
};

/// The internal topic a krabka broker publishes barrier state to.
pub const BARRIER_STATE_TOPIC: &str = "__barrier_state";

/// The record version that both the key and the cut value carry.
const RECORD_VERSION: i16 = 0;

/// The key kind of a cut record. Kinds `0` and `1` are the group definition and
/// the injection start, and a client skips both.
const CUT_KIND: i16 = 2;

/// The part of a record a decode failed on. It names the
/// [`StreamsClientError::BarrierFormat`] `part` field.
const KEY_PART: &str = "key";
const VALUE_PART: &str = "cut value";

/// Whether the coordinator reached every partition of a barrier group.
///
/// The coordinator publishes both outcomes. A complete cut names a marker
/// offset for every partition of the group. A partial cut leaves partitions
/// out, and those partitions never receive that epoch's marker, so a task that
/// waits for one waits forever. Only a complete cut is alignable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CutStatus {
    /// Every partition of the barrier group received the epoch's marker.
    Complete,
    /// One or more partitions never received the epoch's marker.
    Partial,
}

impl CutStatus {
    /// The wire code the coordinator writes for this status.
    #[must_use]
    pub fn code(self) -> i8 {
        match self {
            Self::Complete => 0,
            Self::Partial => 1,
        }
    }

    /// Maps a wire code onto a status.
    ///
    /// # Errors
    ///
    /// Returns [`StreamsClientError::BarrierFormat`] when no status carries the
    /// code.
    pub fn from_code(code: i8) -> Result<Self, StreamsClientError> {
        match code {
            0 => Ok(Self::Complete),
            1 => Ok(Self::Partial),
            other => Err(malformed(
                VALUE_PART,
                format!("unknown barrier cut status {other}"),
            )),
        }
    }
}

/// One epoch's cut across every partition of a barrier group.
///
/// A cut names an exact position in every input at once: the offset of the
/// epoch's marker in each partition. The marker is a Kafka control record, so no
/// consumer receives it and the cut offset never holds a data record. The
/// records before the cut of a partition are exactly the records with a lower
/// offset.
///
/// [`CutReader`](super::CutReader) decodes cuts from [`BARRIER_STATE_TOPIC`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarrierCut {
    /// The barrier group the coordinator injected markers for.
    pub group: String,
    /// The epoch that names this cut inside the group. A coordinator never
    /// reuses one.
    pub epoch: i64,
    /// The time the injection started, in milliseconds since the Unix epoch.
    pub triggered_at: i64,
    /// The time the coordinator published the cut, in milliseconds since the
    /// Unix epoch.
    pub completed_at: i64,
    /// Whether every partition received the epoch's marker.
    pub status: CutStatus,
    /// The marker offset of every partition that received one, as topic to
    /// partition to offset.
    pub offsets: Position,
    /// The partitions that never received the epoch's marker. It is empty for a
    /// complete cut.
    pub missing: BTreeSet<TopicPartition>,
}

impl BarrierCut {
    /// Whether every partition of the group received the epoch's marker.
    ///
    /// A runner aligns on a complete cut only.
    #[must_use]
    pub fn complete(&self) -> bool {
        self.status == CutStatus::Complete
    }

    /// The marker offset of one partition, or `None` when the cut does not name
    /// it.
    #[must_use]
    pub fn offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.offsets.offset(topic, partition)
    }

    /// Whether a partition delivered everything below the cut.
    ///
    /// `position` is the next offset the task reads. A partition the cut does
    /// not name holds nothing back, so it always reports `true`.
    #[must_use]
    pub fn reached(&self, topic: &str, partition: i32, position: i64) -> bool {
        self.offset(topic, partition)
            .is_none_or(|offset| position >= offset)
    }

    /// Every partition the cut names, in topic and then partition order.
    #[must_use]
    pub fn partitions(&self) -> Vec<TopicPartition> {
        self.offsets
            .0
            .iter()
            .flat_map(|(topic, partitions)| {
                partitions.keys().map(|partition| TopicPartition {
                    topic: topic.clone(),
                    partition: *partition,
                })
            })
            .collect()
    }
}

/// A decoded key of the barrier state topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BarrierKey {
    pub kind: i16,
    pub group: String,
    pub epoch: i64,
}

impl BarrierKey {
    /// Whether this key names a cut record.
    pub(crate) fn is_cut(&self) -> bool {
        self.kind == CUT_KIND
    }
}

fn malformed(part: &'static str, message: String) -> StreamsClientError {
    StreamsClientError::BarrierFormat { part, message }
}

/// Decodes one record of [`BARRIER_STATE_TOPIC`] into a cut.
///
/// The result is `None` for a record that carries no cut: a group definition, an
/// injection start, and a tombstone of any kind.
///
/// # Errors
///
/// Returns [`StreamsClientError::BarrierFormat`] when the key or the value does
/// not match the frozen layout. The `part` field names which of the two failed.
pub fn decode_barrier_cut(
    key: &[u8],
    value: Option<&[u8]>,
) -> Result<Option<BarrierCut>, StreamsClientError> {
    let key = decode_key(key)?;
    if !key.is_cut() {
        return Ok(None);
    }
    match value {
        None => Ok(None),
        Some(value) => decode_cut_value(&key, value).map(Some),
    }
}

/// Decodes the key of any barrier state record.
pub(crate) fn decode_key(data: &[u8]) -> Result<BarrierKey, StreamsClientError> {
    let mut reader = Reader::new(data, KEY_PART);
    let version = reader.i16()?;
    if version != RECORD_VERSION {
        return Err(malformed(
            KEY_PART,
            format!("unsupported barrier state key version {version}"),
        ));
    }
    let kind = reader.i16()?;
    let group = reader.string()?;
    let epoch = reader.i64()?;
    reader.finish()?;
    Ok(BarrierKey { kind, group, epoch })
}

/// Decodes the value of a cut record, with the group and the epoch taken from
/// the key.
pub(crate) fn decode_cut_value(
    key: &BarrierKey,
    data: &[u8],
) -> Result<BarrierCut, StreamsClientError> {
    let mut reader = Reader::new(data, VALUE_PART);
    let version = reader.i16()?;
    if version != RECORD_VERSION {
        return Err(malformed(
            VALUE_PART,
            format!("unsupported barrier state cut value version {version}"),
        ));
    }
    let triggered_at = reader.i64()?;
    let completed_at = reader.i64()?;
    let status = CutStatus::from_code(reader.i8()?)?;
    let offsets = reader.cut_offsets()?;
    let missing = reader.missing_partitions()?;
    reader.finish()?;
    Ok(BarrierCut {
        group: key.group.clone(),
        epoch: key.epoch,
        triggered_at,
        completed_at,
        status,
        offsets,
        missing,
    })
}

/// Reads the big-endian layout of the barrier state topic.
///
/// Every integer is signed, and a string is an `i16` byte length and then UTF-8
/// bytes.
struct Reader<'a> {
    data: &'a [u8],
    part: &'static str,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8], part: &'static str) -> Self {
        Self { data, part }
    }

    fn truncated(&self) -> StreamsClientError {
        malformed(self.part, format!("truncated barrier state {}", self.part))
    }

    /// Fails when bytes remain after the last field.
    fn finish(&self) -> Result<(), StreamsClientError> {
        if self.data.is_empty() {
            return Ok(());
        }
        Err(malformed(
            self.part,
            format!("trailing bytes in barrier state {}", self.part),
        ))
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], StreamsClientError> {
        let Some((head, rest)) = self.data.split_at_checked(count) else {
            return Err(self.truncated());
        };
        self.data = rest;
        Ok(head)
    }

    fn i8(&mut self) -> Result<i8, StreamsClientError> {
        Ok(self.take(1)?[0].cast_signed())
    }

    fn i16(&mut self) -> Result<i16, StreamsClientError> {
        let bytes = self.take(2)?;
        Ok(i16::from_be_bytes(bytes.try_into().expect("two bytes")))
    }

    fn i32(&mut self) -> Result<i32, StreamsClientError> {
        let bytes = self.take(4)?;
        Ok(i32::from_be_bytes(bytes.try_into().expect("four bytes")))
    }

    fn i64(&mut self) -> Result<i64, StreamsClientError> {
        let bytes = self.take(8)?;
        Ok(i64::from_be_bytes(bytes.try_into().expect("eight bytes")))
    }

    fn string(&mut self) -> Result<String, StreamsClientError> {
        let length = self.i16()?;
        let length = usize::try_from(length).map_err(|_error| {
            malformed(
                self.part,
                format!(
                    "negative string length {length} in barrier state {}",
                    self.part
                ),
            )
        })?;
        let bytes = self.take(length)?;
        String::from_utf8(bytes.to_vec()).map_err(|_error| {
            malformed(
                self.part,
                format!("non-UTF-8 string in barrier state {}", self.part),
            )
        })
    }

    /// Reads an `i32` array length.
    ///
    /// A negative length is malformed. A length past the remaining bytes fails
    /// on the first entry, so no bogus count allocates.
    fn count(&mut self) -> Result<i32, StreamsClientError> {
        let count = self.i32()?;
        if count < 0 {
            return Err(malformed(
                self.part,
                format!(
                    "negative array length {count} in barrier state {}",
                    self.part
                ),
            ));
        }
        Ok(count)
    }

    fn cut_offsets(&mut self) -> Result<Position, StreamsClientError> {
        let mut offsets: BTreeMap<String, BTreeMap<i32, i64>> = BTreeMap::new();
        for _ in 0..self.count()? {
            let topic = self.string()?;
            let partitions = self.count()?;
            let entry = offsets.entry(topic).or_default();
            for _ in 0..partitions {
                let partition = self.i32()?;
                entry.insert(partition, self.i64()?);
            }
        }
        Ok(Position(offsets))
    }

    fn missing_partitions(&mut self) -> Result<BTreeSet<TopicPartition>, StreamsClientError> {
        let mut missing = BTreeSet::new();
        for _ in 0..self.count()? {
            let topic = self.string()?;
            missing.insert(TopicPartition {
                topic,
                partition: self.i32()?,
            });
        }
        Ok(missing)
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::barrier::testing::{CutRecord, cut_key, cut_value};

    #[test]
    fn decodes_a_complete_cut_from_its_frozen_bytes() {
        let record = CutRecord {
            group: "transactions".to_owned(),
            epoch: 7,
            triggered_at: 100,
            completed_at: 140,
            status: CutStatus::Complete,
            offsets: vec![("orders", 0, 12), ("orders", 1, 5), ("payments", 0, 31)],
            missing: vec![],
        };
        let decoded = decode_barrier_cut(&cut_key(&record), Some(&cut_value(&record)))
            .unwrap()
            .unwrap();
        check!(decoded == record.expected());
    }

    #[test]
    fn decodes_a_partial_cut_with_its_missing_partitions() {
        let record = CutRecord {
            group: "transactions".to_owned(),
            epoch: 8,
            triggered_at: 200,
            completed_at: 260,
            status: CutStatus::Partial,
            offsets: vec![("orders", 0, 40)],
            missing: vec![("orders", 1)],
        };
        let decoded = decode_barrier_cut(&cut_key(&record), Some(&cut_value(&record)))
            .unwrap()
            .unwrap();
        check!(decoded == record.expected());
        check!(!decoded.complete());
    }

    #[test]
    fn skips_other_kinds_and_tombstones() {
        let record = CutRecord::simple("g", 3, &[("in", 0, 1)]);
        // Kind 0 is a group definition and kind 1 is an injection start.
        for kind in [0i16, 1] {
            let key = crate::barrier::testing::key_of_kind(kind, "g", 3);
            check!(decode_barrier_cut(&key, Some(&cut_value(&record))).unwrap() == None);
        }
        check!(decode_barrier_cut(&cut_key(&record), None).unwrap() == None);
    }

    #[test]
    fn cut_answers_offset_reached_and_partitions() {
        let record = CutRecord::simple("g", 1, &[("in", 0, 10), ("in", 1, 4)]);
        let cut = record.expected();
        check!(cut.offset("in", 0) == Some(10));
        check!(cut.offset("in", 2) == None);
        check!(!cut.reached("in", 0, 9));
        check!(cut.reached("in", 0, 10));
        check!(cut.reached("other", 0, 0));
        check!(
            cut.partitions()
                == vec![
                    TopicPartition {
                        topic: "in".to_owned(),
                        partition: 0
                    },
                    TopicPartition {
                        topic: "in".to_owned(),
                        partition: 1
                    },
                ]
        );
    }

    #[test]
    fn rejects_malformed_records() {
        let record = CutRecord::simple("g", 1, &[("in", 0, 10)]);
        let key = cut_key(&record);
        let value = cut_value(&record);

        let mut bad_key_version = key.to_vec();
        bad_key_version[1] = 9;
        assert!(let Err(error) = decode_barrier_cut(&bad_key_version, Some(&value)));
        check!(
            error.to_string()
                == "malformed barrier state key: unsupported barrier state key version 9"
        );

        assert!(let Err(error) = decode_barrier_cut(&key[..key.len() - 1], Some(&value)));
        check!(error.to_string().contains("truncated barrier state key"));

        let mut trailing_key = key.to_vec();
        trailing_key.push(0);
        assert!(let Err(error) = decode_barrier_cut(&trailing_key, Some(&value)));
        check!(
            error
                .to_string()
                .contains("trailing bytes in barrier state key")
        );

        let mut bad_status = value.to_vec();
        bad_status[18] = 7;
        assert!(let Err(error) = decode_barrier_cut(&key, Some(&bad_status)));
        check!(error.to_string().contains("unknown barrier cut status 7"));

        assert!(let Err(error) = decode_barrier_cut(&key, Some(&value[..value.len() - 2])));
        check!(
            error
                .to_string()
                .contains("truncated barrier state cut value")
        );

        let mut negative_count = value.to_vec();
        // The topics array length sits right after version, the two timestamps,
        // and the status byte.
        negative_count[19] = 0xFF;
        assert!(let Err(error) = decode_barrier_cut(&key, Some(&negative_count)));
        check!(error.to_string().contains("negative array length"));
    }

    #[test]
    fn status_codes_round_trip() {
        for status in [CutStatus::Complete, CutStatus::Partial] {
            check!(CutStatus::from_code(status.code()).unwrap() == status);
        }
        assert!(let Err(error) = CutStatus::from_code(2));
        check!(error.to_string().contains("unknown barrier cut status 2"));
    }
}
