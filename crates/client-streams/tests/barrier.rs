//! Public behaviour of the barrier primitive: the frozen `__barrier_state`
//! layouts, the cut reader, and the frozen snapshot container.
//!
//! Every byte fixture here is written out by hand from the barrier design's
//! "Wire Formats" section, so the suite pins the format itself and not the
//! encoder that a test helper would share with the decoder.
//!
//! The end-to-end path against a live broker is not here. The broker's barrier
//! coordinator is not in the revision that the root `Cargo.toml`
//! `[patch.crates-io]` block pins, so no reachable broker publishes a cut.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use assert2::{assert, check};
use bytes::Bytes;
use krabka_client_streams::{
    BARRIER_STATE_TOPIC, BarrierCut, CutReader, CutStatus, FileSnapshotStore, Position,
    SnapshotKey, SnapshotStore, StreamsClientError, TaskSnapshot, TopicPartition,
    decode_barrier_cut,
    runtime::{FetchBatch, FetchedRec, IsolationLevel, RecordFetcher},
    store::snapshot::{SNAPSHOT_CONTAINER_VERSION, decode_snapshot, encode_snapshot},
};

/// The key of the cut of group `txns` at epoch 7.
///
/// ```text
/// version i16 = 0 | kind i16 = 2 | group string | epoch i64
/// ```
const CUT_KEY: &[u8] = &[
    0x00, 0x00, // version 0
    0x00, 0x02, // kind 2, a cut
    0x00, 0x04, b't', b'x', b'n', b's', // group "txns"
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // epoch 7
];

/// The value of that cut: `in-0` at offset 12 and `in-1` at offset 5.
///
/// ```text
/// version i16 = 0 | triggered_at i64 | completed_at i64 | status i8
/// topics i32 [ topic string | partitions i32 [ partition i32 | offset i64 ] ]
/// missing i32 [ topic string | partition i32 ]
/// ```
const CUT_VALUE: &[u8] = &[
    0x00, 0x00, // version 0
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, // triggered_at 100
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x8C, // completed_at 140
    0x00, // status 0, complete
    0x00, 0x00, 0x00, 0x01, // one topic
    0x00, 0x02, b'i', b'n', // topic "in"
    0x00, 0x00, 0x00, 0x02, // two partitions
    0x00, 0x00, 0x00, 0x00, // partition 0
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0C, // offset 12
    0x00, 0x00, 0x00, 0x01, // partition 1
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, // offset 5
    0x00, 0x00, 0x00, 0x00, // no missing partitions
];

/// The same epoch published as a partial cut: `in-0` got a marker, `in-1` did
/// not.
const PARTIAL_CUT_VALUE: &[u8] = &[
    0x00, 0x00, // version 0
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, // triggered_at 100
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x8C, // completed_at 140
    0x01, // status 1, partial
    0x00, 0x00, 0x00, 0x01, // one topic
    0x00, 0x02, b'i', b'n', // topic "in"
    0x00, 0x00, 0x00, 0x01, // one partition
    0x00, 0x00, 0x00, 0x00, // partition 0
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0C, // offset 12
    0x00, 0x00, 0x00, 0x01, // one missing partition
    0x00, 0x02, b'i', b'n', // topic "in"
    0x00, 0x00, 0x00, 0x01, // partition 1
];

/// A group-definition key, which a client skips.
const GROUP_KEY: &[u8] = &[
    0x00, 0x00, // version 0
    0x00, 0x00, // kind 0, a group definition
    0x00, 0x04, b't', b'x', b'n', b's', // group "txns"
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // epoch -1
];

fn position(entries: &[(&str, i32, i64)]) -> Position {
    let mut map: BTreeMap<String, BTreeMap<i32, i64>> = BTreeMap::new();
    for (topic, partition, offset) in entries {
        map.entry((*topic).to_owned())
            .or_default()
            .insert(*partition, *offset);
    }
    Position(map)
}

fn complete_cut() -> BarrierCut {
    BarrierCut {
        group: "txns".to_owned(),
        epoch: 7,
        triggered_at: 100,
        completed_at: 140,
        status: CutStatus::Complete,
        offsets: position(&[("in", 0, 12), ("in", 1, 5)]),
        missing: BTreeSet::new(),
    }
}

#[test]
fn decodes_a_cut_from_the_frozen_bytes() {
    assert!(let Ok(Some(cut)) = decode_barrier_cut(CUT_KEY, Some(CUT_VALUE)));
    check!(cut == complete_cut());
    check!(cut.complete());
    check!(cut.offset("in", 0) == Some(12));
    check!(cut.reached("in", 1, 5));
    check!(!cut.reached("in", 1, 4));
}

#[test]
fn decodes_a_partial_cut_with_its_missing_partitions() {
    assert!(let Ok(Some(cut)) = decode_barrier_cut(CUT_KEY, Some(PARTIAL_CUT_VALUE)));
    check!(
        cut == BarrierCut {
            group: "txns".to_owned(),
            epoch: 7,
            triggered_at: 100,
            completed_at: 140,
            status: CutStatus::Partial,
            offsets: position(&[("in", 0, 12)]),
            missing: [TopicPartition {
                topic: "in".to_owned(),
                partition: 1,
            }]
            .into_iter()
            .collect(),
        }
    );
    check!(!cut.complete());
}

#[test]
fn skips_a_record_that_carries_no_cut() {
    // A group definition is kind 0, and a tombstone has no value.
    check!(decode_barrier_cut(GROUP_KEY, Some(CUT_VALUE)).unwrap() == None);
    check!(decode_barrier_cut(CUT_KEY, None).unwrap() == None);
}

/// One malformed-record case: what it is, the key, the value, and the reason
/// the decoder must give.
struct MalformedCase {
    name: &'static str,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    reason: &'static str,
}

#[test]
fn rejects_malformed_barrier_state() {
    let cases = vec![
        MalformedCase {
            name: "truncated key",
            key: CUT_KEY[..CUT_KEY.len() - 1].to_vec(),
            value: Some(CUT_VALUE.to_vec()),
            reason: "truncated barrier state key",
        },
        MalformedCase {
            name: "trailing key bytes",
            key: [CUT_KEY, &[0]].concat(),
            value: Some(CUT_VALUE.to_vec()),
            reason: "trailing bytes in barrier state key",
        },
        MalformedCase {
            name: "truncated value",
            key: CUT_KEY.to_vec(),
            value: Some(CUT_VALUE[..CUT_VALUE.len() - 3].to_vec()),
            reason: "truncated barrier state cut value",
        },
        MalformedCase {
            name: "trailing value bytes",
            key: CUT_KEY.to_vec(),
            value: Some([CUT_VALUE, &[0]].concat()),
            reason: "trailing bytes in barrier state cut value",
        },
    ];
    for case in cases {
        let name = case.name;
        assert!(
            let Err(error) = decode_barrier_cut(&case.key, case.value.as_deref()),
            "{name} must be rejected"
        );
        check!(error.to_string().contains(case.reason), "{name}");
    }

    // An unknown status code is malformed too.
    let mut unknown_status = CUT_VALUE.to_vec();
    unknown_status[18] = 4;
    assert!(let Err(error) = decode_barrier_cut(CUT_KEY, Some(&unknown_status)));
    check!(error.to_string().contains("unknown barrier cut status 4"));
}

/// Serves a scripted `__barrier_state` partition to a [`CutReader`].
struct BarrierStateFetcher {
    records: Mutex<Vec<FetchedRec>>,
}

impl BarrierStateFetcher {
    fn new(records: Vec<FetchedRec>) -> Arc<Self> {
        Arc::new(Self {
            records: Mutex::new(records),
        })
    }
}

#[async_trait::async_trait]
impl RecordFetcher for BarrierStateFetcher {
    async fn fetch(
        &self,
        topic: &str,
        _partition: i32,
        offset: i64,
        _isolation: IsolationLevel,
    ) -> Result<FetchBatch, StreamsClientError> {
        // Serve the whole script once, then report a caught-up partition. A
        // reader that asked for another topic gets nothing.
        if offset > 0 || topic != BARRIER_STATE_TOPIC {
            return Ok(FetchBatch::default());
        }
        Ok(FetchBatch {
            records: std::mem::take(&mut self.records.lock().unwrap()),
        })
    }
}

fn cut_record(offset: i64, key: &[u8], value: &[u8]) -> FetchedRec {
    FetchedRec {
        offset,
        key: Some(Bytes::copy_from_slice(key)),
        value: Some(Bytes::copy_from_slice(value)),
        timestamp: -1,
    }
}

/// The key of a cut of group `txns` at any epoch.
fn key_at(epoch: i64) -> Vec<u8> {
    let mut key = CUT_KEY[..10].to_vec();
    key.extend_from_slice(&epoch.to_be_bytes());
    key
}

/// A complete cut value whose single partition sits at `offset`.
fn value_at(offset: i64) -> Vec<u8> {
    let mut value = CUT_VALUE.to_vec();
    // Rewrite `in-1`'s offset, the last one in the topics array.
    let start = value.len() - 12;
    value[start..start + 8].copy_from_slice(&offset.to_be_bytes());
    value
}

#[tokio::test]
async fn reader_serves_the_latest_complete_cut_and_the_cuts_after_an_epoch() {
    let fetcher = BarrierStateFetcher::new(vec![
        cut_record(0, &key_at(7), CUT_VALUE),
        cut_record(1, &key_at(8), &value_at(9)),
    ]);
    let mut reader = CutReader::new(fetcher);

    assert!(let Some(latest) = reader.latest_complete_cut("txns").await.unwrap());
    check!(latest.epoch == 8);
    check!(latest.offset("in", 1) == Some(9));

    let after = reader.complete_cuts_after("txns", 7).await.unwrap();
    check!(after.len() == 1);
    check!(after[0].epoch == 8);

    let all = reader.complete_cuts_after("txns", -1).await.unwrap();
    check!(all.iter().map(|cut| cut.epoch).collect::<Vec<_>>() == vec![7, 8]);
    check!(all[0] == complete_cut());

    check!(reader.latest_complete_cut("other").await.unwrap() == None);
}

#[tokio::test]
async fn reader_never_serves_a_partial_cut() {
    let fetcher = BarrierStateFetcher::new(vec![
        cut_record(0, &key_at(7), PARTIAL_CUT_VALUE),
        cut_record(1, &key_at(8), CUT_VALUE),
        // A group definition shares the topic and is skipped.
        cut_record(2, GROUP_KEY, CUT_VALUE),
    ]);
    let mut reader = CutReader::new(fetcher);

    let cuts = reader.complete_cuts_after("txns", -1).await.unwrap();
    check!(cuts.iter().map(|cut| cut.epoch).collect::<Vec<_>>() == vec![8]);
    assert!(let Some(latest) = reader.latest_complete_cut("txns").await.unwrap());
    check!(latest.epoch == 8);
}

#[test]
fn snapshot_container_bytes_match_the_frozen_layout() {
    let mut snapshot = TaskSnapshot::new();
    // Inserted out of order: the container writes them by ascending name.
    snapshot.insert("windows".to_owned(), Bytes::from_static(&[0x02, 0x02]));
    snapshot.insert("counts".to_owned(), Bytes::from_static(&[0x01]));

    let expected: Vec<u8> = [
        &[0x00, 0x00, 0x00, 0x01][..], // version 1
        &[0x00, 0x00, 0x00, 0x02],     // two entries
        &[0x00, 0x00, 0x00, 0x06],     // name length 6
        b"counts",
        &[0x00, 0x00, 0x00, 0x01], // value length 1
        &[0x01],
        &[0x00, 0x00, 0x00, 0x07], // name length 7
        b"windows",
        &[0x00, 0x00, 0x00, 0x02], // value length 2
        &[0x02, 0x02],
    ]
    .concat();

    check!(SNAPSHOT_CONTAINER_VERSION == 1);
    check!(encode_snapshot(&snapshot).as_ref() == expected.as_slice());
    check!(decode_snapshot(&expected).unwrap() == snapshot);
}

#[test]
fn snapshot_container_rejects_malformed_bytes() {
    let encoded = encode_snapshot(&TaskSnapshot::new());
    assert!(let Err(error) = decode_snapshot(&encoded[..3]));
    check!(error.to_string().contains("truncated state snapshot"));

    let mut wrong_version = encoded.to_vec();
    wrong_version[3] = 2;
    assert!(let Err(error) = decode_snapshot(&wrong_version));
    check!(error.to_string() == "state snapshot error: unsupported state snapshot version 2");
}

#[tokio::test]
async fn file_snapshot_store_round_trips_one_epoch_at_a_time() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileSnapshotStore::new(directory.path());
    let mut snapshot = TaskSnapshot::new();
    snapshot.insert("counts".to_owned(), Bytes::from_static(b"\x07"));

    let key = SnapshotKey::new("0-1", "txns", 7);
    check!(store.load(&key).await.unwrap() == None);
    store.save(&key, &snapshot).await.unwrap();
    check!(store.load(&key).await.unwrap() == Some(snapshot.clone()));

    // Another epoch and another task are separate keys.
    check!(
        store
            .load(&SnapshotKey::new("0-1", "txns", 8))
            .await
            .unwrap()
            == None
    );
    check!(
        store
            .load(&SnapshotKey::new("0-2", "txns", 7))
            .await
            .unwrap()
            == None
    );
}

/// The cut format is frozen across `krabka-broker`, `krabka-streams-java`,
/// `krabka-streams-go` and this crate. These are the exact bytes the other
/// three assert, encoded from the documented layout with no implementation in
/// the loop, so a decoder that drifts fails here rather than in production.
///
/// Key: version 0, kind 2, group `orders-cut`, epoch 7. Value: version 0,
/// triggered 1724500000000, completed 1724500000042, status complete, topic
/// `orders` with partition 0 at offset 1024 and partition 1 at offset 2048,
/// and no missing partitions.
const SHARED_GOLDEN_KEY: &[u8] = &[
    0x00, 0x00, 0x00, 0x02, 0x00, 0x0a, 0x6f, 0x72, 0x64, 0x65, 0x72, 0x73, 0x2d, 0x63, 0x75, 0x74,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
];

const SHARED_GOLDEN_VALUE: &[u8] = &[
    0x00, 0x00, 0x00, 0x00, 0x01, 0x91, 0x84, 0x35, 0xbd, 0x00, 0x00, 0x00, 0x01, 0x91, 0x84, 0x35,
    0xbd, 0x2a, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x6f, 0x72, 0x64, 0x65, 0x72, 0x73, 0x00,
    0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00,
    0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00,
];

#[test]
fn decodes_the_golden_vector_the_other_three_implementations_assert() {
    assert!(let Ok(Some(_cut)) = decode_barrier_cut(SHARED_GOLDEN_KEY, Some(SHARED_GOLDEN_VALUE)));
    let cut = decode_barrier_cut(SHARED_GOLDEN_KEY, Some(SHARED_GOLDEN_VALUE))
        .expect("decodes")
        .expect("is a cut");

    check!(cut.group == "orders-cut");
    check!(cut.epoch == 7);
    check!(cut.triggered_at == 1_724_500_000_000);
    check!(cut.completed_at == 1_724_500_000_042);
    check!(cut.complete());
    check!(cut.missing.is_empty());
    check!(cut.offset("orders", 0) == Some(1024));
    check!(cut.offset("orders", 1) == Some(2048));
}
