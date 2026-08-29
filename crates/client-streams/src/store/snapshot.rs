//! Barrier snapshots: the frozen container, the storage seam, and the shared
//! byte-store payload codec.
//!
//! A task snapshots every store it owns when a barrier fires. The result is a
//! map from store name to opaque payload bytes, and the container below holds
//! it. The container is frozen. `krabka-streams-rs`, `krabka-streams-java`, and
//! `krabka-streams-go` write the same bytes:
//!
//! ```text
//! version u32 = 1
//! count   u32
//! entries count x [ name_len u32 | name UTF-8 | value_len u32 | value bytes ]
//! ```
//!
//! Every integer is big-endian. The entries are in ascending byte order of the
//! name, so one snapshot always encodes to one byte string.
//!
//! The payload inside an entry is language-specific. Rust uses the byte-store
//! payload that [`encode_byte_entries`] writes for every store over the
//! pluggable byte backend, and a store with in-process state of its own writes
//! its own payload.
//!
//! The cut identity is not in the container. It is the storage key, which is
//! the task, the barrier group, and the epoch. See [`SnapshotKey`].

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};

use crate::{error::StreamsClientError, store::byte::ByteKeyValueStore};

/// The container version that the three streams libraries share.
pub const SNAPSHOT_CONTAINER_VERSION: u32 = 1;

/// The version of the store payloads that Rust writes inside a container entry.
const STORE_PAYLOAD_VERSION: u32 = 1;

/// Every store of one task at one cut, as store name to payload bytes.
///
/// The map is ordered, and [`encode_snapshot`] writes the entries in that
/// order. `BTreeMap<String, _>` orders by the UTF-8 bytes of the name, which is
/// the order the frozen container needs.
pub type TaskSnapshot = BTreeMap<String, Bytes>;

/// Reads big-endian fields off a snapshot buffer and reports a short reason
/// when the buffer runs out.
///
/// Every store that writes its own payload decodes it with this reader, so a
/// truncated snapshot always reports the same way.
pub(crate) struct SnapshotReader<'a> {
    data: &'a [u8],
}

impl<'a> SnapshotReader<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], StreamsClientError> {
        let Some((head, rest)) = self.data.split_at_checked(count) else {
            return Err(truncated());
        };
        self.data = rest;
        Ok(head)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, StreamsClientError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u32(&mut self) -> Result<u32, StreamsClientError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes(bytes.try_into().expect("four bytes")))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, StreamsClientError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes(bytes.try_into().expect("eight bytes")))
    }

    pub(crate) fn i64(&mut self) -> Result<i64, StreamsClientError> {
        let bytes = self.take(8)?;
        Ok(i64::from_be_bytes(bytes.try_into().expect("eight bytes")))
    }

    /// Reads a `u32` length and then that many bytes.
    pub(crate) fn sized(&mut self) -> Result<Bytes, StreamsClientError> {
        let length = self.u32()? as usize;
        Ok(Bytes::copy_from_slice(self.take(length)?))
    }

    /// Fails when bytes remain after the last field.
    pub(crate) fn finish(&self) -> Result<(), StreamsClientError> {
        if self.is_empty() {
            return Ok(());
        }
        Err(StreamsClientError::Snapshot(
            "trailing bytes in state snapshot".to_owned(),
        ))
    }

    /// Checks the leading version field of a store's own payload.
    pub(crate) fn store_version(&mut self) -> Result<(), StreamsClientError> {
        let version = self.u32()?;
        if version == STORE_PAYLOAD_VERSION {
            return Ok(());
        }
        Err(StreamsClientError::Snapshot(format!(
            "unsupported store snapshot version {version}"
        )))
    }
}

fn truncated() -> StreamsClientError {
    StreamsClientError::Snapshot("truncated state snapshot".to_owned())
}

/// Writes a `u32` length and then the bytes.
///
/// # Panics
///
/// Panics when `value` is longer than `u32::MAX` bytes.
pub(crate) fn put_sized(buffer: &mut BytesMut, value: &[u8]) {
    buffer.put_u32(u32::try_from(value.len()).expect("snapshot entry length fits u32"));
    buffer.extend_from_slice(value);
}

/// Starts a store's own payload with the payload version.
pub(crate) fn store_payload() -> BytesMut {
    let mut buffer = BytesMut::new();
    buffer.put_u32(STORE_PAYLOAD_VERSION);
    buffer
}

/// Encodes a task snapshot into the frozen container bytes.
///
/// The entries come out in ascending byte order of the store name, so the same
/// snapshot always gives the same bytes.
///
/// # Panics
///
/// Panics when a store name or a payload is longer than `u32::MAX` bytes.
#[must_use]
pub fn encode_snapshot(snapshot: &TaskSnapshot) -> Bytes {
    let mut buffer = BytesMut::new();
    buffer.put_u32(SNAPSHOT_CONTAINER_VERSION);
    buffer.put_u32(u32::try_from(snapshot.len()).expect("snapshot entry count fits u32"));
    for (name, value) in snapshot {
        put_sized(&mut buffer, name.as_bytes());
        put_sized(&mut buffer, value);
    }
    buffer.freeze()
}

/// Decodes the frozen container bytes back into a task snapshot.
///
/// # Errors
///
/// Returns [`StreamsClientError::Snapshot`] when the version is not
/// [`SNAPSHOT_CONTAINER_VERSION`], when a length runs past the end of the
/// buffer, when a name is not UTF-8, or when bytes remain after the last entry.
pub fn decode_snapshot(data: &[u8]) -> Result<TaskSnapshot, StreamsClientError> {
    let mut reader = SnapshotReader::new(data);
    let version = reader.u32()?;
    if version != SNAPSHOT_CONTAINER_VERSION {
        return Err(StreamsClientError::Snapshot(format!(
            "unsupported state snapshot version {version}"
        )));
    }
    let count = reader.u32()?;
    let mut snapshot = TaskSnapshot::new();
    for _ in 0..count {
        let name = reader.sized()?;
        let name = String::from_utf8(name.to_vec())
            .map_err(|_error| StreamsClientError::Snapshot("non-UTF-8 store name".to_owned()))?;
        snapshot.insert(name, reader.sized()?);
    }
    reader.finish()?;
    Ok(snapshot)
}

/// Encodes the entries of a byte-backed store as one entry payload.
///
/// The layout is a `u32` version of `1`, a `u32` entry count, and then per
/// entry a `u32` key length, the key, a `u32` value length, and the value. The
/// payload is Rust's own, so no other language reads it.
///
/// # Panics
///
/// Panics when a key or a value is longer than `u32::MAX` bytes.
pub(crate) fn encode_byte_entries(entries: &[(Bytes, Bytes)]) -> Bytes {
    let mut buffer = store_payload();
    put_byte_entries(&mut buffer, entries);
    buffer.freeze()
}

/// Writes a count-prefixed run of key-value entries into a store payload.
///
/// A store that also carries scalars writes them first and then calls this
/// function, so every byte-backed store shares one entry layout.
///
/// # Panics
///
/// Panics when there are more than `u32::MAX` entries.
pub(crate) fn put_byte_entries(buffer: &mut BytesMut, entries: &[(Bytes, Bytes)]) {
    buffer.put_u32(u32::try_from(entries.len()).expect("entry count fits u32"));
    for (key, value) in entries {
        put_sized(buffer, key);
        put_sized(buffer, value);
    }
}

/// Decodes the payload that [`encode_byte_entries`] wrote.
pub(crate) fn decode_byte_entries(data: &[u8]) -> Result<Vec<(Bytes, Bytes)>, StreamsClientError> {
    let mut reader = SnapshotReader::new(data);
    reader.store_version()?;
    let entries = read_byte_entries(&mut reader)?;
    reader.finish()?;
    Ok(entries)
}

/// Reads a count-prefixed run of key-value entries out of a store payload.
pub(crate) fn read_byte_entries(
    reader: &mut SnapshotReader<'_>,
) -> Result<Vec<(Bytes, Bytes)>, StreamsClientError> {
    let count = reader.u32()?;
    let mut entries = Vec::new();
    for _ in 0..count {
        let key = reader.sized()?;
        entries.push((key, reader.sized()?));
    }
    Ok(entries)
}

/// Snapshots every entry of a byte-backed store.
///
/// Each store over the pluggable byte backend delegates its
/// [`StateStore::snapshot`](crate::store::api::StateStore::snapshot) to this
/// function, so all of them share one payload format.
pub(crate) async fn snapshot_byte_store(store: &dyn ByteKeyValueStore) -> Bytes {
    encode_byte_entries(&store.scan_all().await)
}

/// Replaces every entry of a byte-backed store with a snapshot payload.
///
/// The store is wiped first, so the restored state is the snapshot and nothing
/// else. A malformed payload leaves the store untouched, because the decode
/// runs before the wipe.
pub(crate) async fn restore_byte_store(
    store: &mut dyn ByteKeyValueStore,
    data: &[u8],
) -> Result<(), StreamsClientError> {
    let entries = decode_byte_entries(data)?;
    store.clear().await;
    for (key, value) in entries {
        store.put(key, value).await;
    }
    Ok(())
}

/// Names one stored snapshot: the task, the barrier group, and the epoch.
///
/// The cut identity lives here and not in the container, so two barrier groups
/// and two epochs never share a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SnapshotKey {
    /// The task the snapshot belongs to, as `<subtopology>-<partition>`.
    pub task: String,
    /// The barrier group whose cut the task aligned on.
    pub group: String,
    /// The epoch of that cut.
    pub epoch: i64,
}

impl SnapshotKey {
    /// Builds a key for one task, group, and epoch.
    #[must_use]
    pub fn new(task: impl Into<String>, group: impl Into<String>, epoch: i64) -> Self {
        Self {
            task: task.into(),
            group: group.into(),
            epoch,
        }
    }

    /// The file name that [`FileSnapshotStore`] gives this key.
    ///
    /// Every character outside `A-Z`, `a-z`, `0-9`, `.`, `_`, and `-` becomes an
    /// underscore, so a group name or a task name can never escape the
    /// directory.
    #[must_use]
    pub fn file_name(&self) -> String {
        format!(
            "{}-{}-epoch-{}.snapshot",
            sanitize(&self.group),
            sanitize(&self.task),
            self.epoch
        )
    }
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Keeps the store snapshots a task takes at each barrier cut.
///
/// [`FileSnapshotStore`] writes them to local files. [`NoSnapshotStore`] keeps
/// nothing, which makes the barrier an alignment and commit point with no
/// durable state.
#[async_trait]
pub trait SnapshotStore: Send + Sync + 'static {
    /// Stores one task's snapshot under `key`, and replaces what was there.
    ///
    /// # Errors
    ///
    /// Returns [`StreamsClientError::Snapshot`] when the implementation cannot
    /// write the snapshot.
    async fn save(
        &self,
        key: &SnapshotKey,
        snapshot: &TaskSnapshot,
    ) -> Result<(), StreamsClientError>;

    /// Reads back the snapshot stored under `key`, or `None` when there is
    /// none.
    ///
    /// # Errors
    ///
    /// Returns [`StreamsClientError::Snapshot`] when the implementation cannot
    /// read the snapshot, or when the stored bytes are malformed.
    async fn load(&self, key: &SnapshotKey) -> Result<Option<TaskSnapshot>, StreamsClientError>;
}

/// A snapshot store that keeps nothing.
///
/// Every save is discarded and every load is empty. Use it when a barrier only
/// has to align and commit, and no cut has to be restorable.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoSnapshotStore;

#[async_trait]
impl SnapshotStore for NoSnapshotStore {
    async fn save(
        &self,
        _key: &SnapshotKey,
        _snapshot: &TaskSnapshot,
    ) -> Result<(), StreamsClientError> {
        Ok(())
    }

    async fn load(&self, _key: &SnapshotKey) -> Result<Option<TaskSnapshot>, StreamsClientError> {
        Ok(None)
    }
}

/// Writes each snapshot to one file under a directory.
///
/// A save writes a temporary file in the same directory and renames it over the
/// target, so a crash in the middle of a save leaves the previous snapshot
/// whole. The directory is created on the first save.
///
/// # Examples
///
/// ```
/// use krabka_client_streams::store::snapshot::{FileSnapshotStore, SnapshotKey};
///
/// let store = FileSnapshotStore::new(std::env::temp_dir().join("krabka-snapshots"));
/// // transactions-0-1-epoch-7.snapshot
/// let file = store.file(&SnapshotKey::new("0-1", "transactions", 7));
/// ```
#[derive(Debug, Clone)]
pub struct FileSnapshotStore {
    directory: PathBuf,
}

impl FileSnapshotStore {
    /// Creates a store rooted at `directory`.
    #[must_use]
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    /// The file that holds one key's snapshot.
    #[must_use]
    pub fn file(&self, key: &SnapshotKey) -> PathBuf {
        self.directory.join(key.file_name())
    }
}

/// Writes `data` to a temporary file beside `target` and renames it over
/// `target`.
fn write_atomically(directory: &Path, target: &Path, data: &[u8]) -> std::io::Result<()> {
    std::fs::create_dir_all(directory)?;
    let mut temporary = target.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    std::fs::write(&temporary, data)?;
    match std::fs::rename(&temporary, target) {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = std::fs::remove_file(&temporary);
            Err(error)
        }
    }
}

#[async_trait]
impl SnapshotStore for FileSnapshotStore {
    async fn save(
        &self,
        key: &SnapshotKey,
        snapshot: &TaskSnapshot,
    ) -> Result<(), StreamsClientError> {
        let directory = self.directory.clone();
        let target = self.file(key);
        let data = encode_snapshot(snapshot);
        let task = key.task.clone();
        tokio::task::spawn_blocking(move || write_atomically(&directory, &target, &data))
            .await
            .map_err(|error| StreamsClientError::Snapshot(error.to_string()))?
            .map_err(|error| {
                StreamsClientError::Snapshot(format!("cannot save task {task} state: {error}"))
            })
    }

    async fn load(&self, key: &SnapshotKey) -> Result<Option<TaskSnapshot>, StreamsClientError> {
        let target = self.file(key);
        let task = key.task.clone();
        let read = tokio::task::spawn_blocking(move || std::fs::read(&target))
            .await
            .map_err(|error| StreamsClientError::Snapshot(error.to_string()))?;
        match read {
            Ok(data) => decode_snapshot(&data).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(StreamsClientError::Snapshot(format!(
                "cannot load task {task} state: {error}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn snapshot(entries: &[(&str, &[u8])]) -> TaskSnapshot {
        entries
            .iter()
            .map(|(name, value)| ((*name).to_owned(), Bytes::copy_from_slice(value)))
            .collect()
    }

    #[test]
    fn container_bytes_match_the_frozen_layout() {
        let encoded = encode_snapshot(&snapshot(&[("b", b"22"), ("a", b"1")]));
        // version 1, count 2, then "a" before "b" whatever the insert order was.
        let expected: Vec<u8> = [
            &1u32.to_be_bytes()[..],
            &2u32.to_be_bytes()[..],
            &1u32.to_be_bytes()[..],
            b"a",
            &1u32.to_be_bytes()[..],
            b"1",
            &1u32.to_be_bytes()[..],
            b"b",
            &2u32.to_be_bytes()[..],
            b"22",
        ]
        .concat();
        check!(encoded.as_ref() == expected.as_slice());
    }

    #[test]
    fn container_round_trips_and_rejects_malformed_bytes() {
        let original = snapshot(&[("counts", b"\x00\x01"), ("windows", b"")]);
        let encoded = encode_snapshot(&original);
        check!(decode_snapshot(&encoded).unwrap() == original);

        let mut wrong_version = encoded.to_vec();
        wrong_version[3] = 9;
        assert!(let Err(error) = decode_snapshot(&wrong_version));
        check!(
            error
                .to_string()
                .contains("unsupported state snapshot version")
        );

        let truncated = &encoded[..encoded.len() - 1];
        assert!(let Err(error) = decode_snapshot(truncated));
        check!(error.to_string().contains("truncated"));

        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert!(let Err(error) = decode_snapshot(&trailing));
        check!(error.to_string().contains("trailing bytes"));
    }

    #[test]
    fn byte_entries_round_trip_and_reject_malformed_bytes() {
        let entries = vec![
            (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
            (Bytes::from_static(b"k2"), Bytes::from_static(b"")),
        ];
        let encoded = encode_byte_entries(&entries);
        check!(decode_byte_entries(&encoded).unwrap() == entries);
        assert!(let Err(error) = decode_byte_entries(&encoded[..5]));
        check!(error.to_string().contains("truncated"));
    }

    #[test]
    fn snapshot_key_file_name_sanitizes_separators() {
        let key = SnapshotKey::new("0/1", "../escape", 3);
        check!(key.file_name() == ".._escape-0_1-epoch-3.snapshot");
    }

    #[tokio::test]
    async fn file_store_saves_loads_and_reports_a_missing_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let store = FileSnapshotStore::new(directory.path());
        let key = SnapshotKey::new("0-0", "transactions", 4);
        check!(store.load(&key).await.unwrap() == None);

        let original = snapshot(&[("counts", b"\x07")]);
        store.save(&key, &original).await.unwrap();
        check!(store.load(&key).await.unwrap() == Some(original.clone()));

        // A second save replaces the file rather than appending to it.
        let replacement = snapshot(&[("counts", b"\x09"), ("other", b"x")]);
        store.save(&key, &replacement).await.unwrap();
        check!(store.load(&key).await.unwrap() == Some(replacement));

        // Another epoch is another file.
        check!(
            store
                .load(&SnapshotKey::new("0-0", "transactions", 5))
                .await
                .unwrap()
                == None
        );
    }

    #[tokio::test]
    async fn no_snapshot_store_keeps_nothing() {
        let store = NoSnapshotStore;
        let key = SnapshotKey::new("0-0", "g", 1);
        store.save(&key, &snapshot(&[("s", b"v")])).await.unwrap();
        check!(store.load(&key).await.unwrap() == None);
    }

    #[tokio::test]
    async fn byte_store_snapshot_replaces_every_entry() {
        use crate::store::byte::InMemoryBytes;

        let mut source = InMemoryBytes::default();
        source
            .put(Bytes::from_static(b"a"), Bytes::from_static(b"1"))
            .await;
        source
            .put(Bytes::from_static(b"b"), Bytes::from_static(b"2"))
            .await;
        let payload = snapshot_byte_store(&source).await;

        let mut target = InMemoryBytes::default();
        target
            .put(Bytes::from_static(b"stale"), Bytes::from_static(b"x"))
            .await;
        restore_byte_store(&mut target, &payload).await.unwrap();
        check!(target.scan_all().await == source.scan_all().await);
    }

    // ─── every store kind ─────────────────────────────────────────────────────

    /// Rewinds a store to `at_cut` and checks it is back where it was, byte for
    /// byte.
    ///
    /// `after` is the payload of the state the store reached past the cut. It
    /// must differ, or the round trip would prove nothing.
    async fn rewinds_to_its_cut(store: &mut dyn crate::store::api::StateStore, at_cut: &Bytes) {
        let after = store.snapshot().await;
        check!(&after != at_cut, "the store must move past the cut");
        store.restore_snapshot(at_cut.clone()).await.unwrap();
        check!(&store.snapshot().await == at_cut);
    }

    fn string_serde() -> Box<dyn crate::processor::serde::Serde<String>> {
        Box::new(crate::processor::serde::StringSerde)
    }

    fn i64_serde() -> Box<dyn crate::processor::serde::Serde<i64>> {
        Box::new(crate::processor::serde::I64Serde)
    }

    #[tokio::test]
    async fn key_value_store_rewinds_to_its_cut() {
        use crate::store::{api::KeyValueStore, kv::KeyValueBytesStore};

        let mut store = KeyValueBytesStore::<String, i64>::in_memory(
            "counts".into(),
            string_serde(),
            i64_serde(),
            "counts-changelog".into(),
        );
        store.put("a".into(), 1).await;
        let at_cut = crate::store::api::StateStore::snapshot(&mut store).await;
        store.put("a".into(), 2).await;
        store.put("b".into(), 9).await;
        rewinds_to_its_cut(&mut store, &at_cut).await;
        check!(store.get(&"a".to_string()).await == Some(1));
        check!(store.get(&"b".to_string()).await == None);
    }

    #[tokio::test]
    async fn window_store_rewinds_to_its_cut() {
        use krabka_units::prelude::*;

        use crate::store::window::{WindowBytesStore, WindowStore};

        let mut store = WindowBytesStore::<String, i64>::in_memory(
            "windows".into(),
            string_serde(),
            i64_serde(),
            "windows-changelog".into(),
            millis(100),
        );
        store.put("a".into(), 0, 1, 5).await;
        let at_cut = crate::store::api::StateStore::snapshot(&mut store).await;
        store.put("a".into(), 100, 7, 105).await;
        rewinds_to_its_cut(&mut store, &at_cut).await;
        check!(store.fetch_single(&"a".to_string(), 100).await == None);
    }

    #[tokio::test]
    async fn session_store_rewinds_to_its_cut() {
        use crate::store::session::{SessionBytesStore, SessionStore};

        let mut store = SessionBytesStore::<String, i64>::in_memory(
            "sessions".into(),
            string_serde(),
            i64_serde(),
            "sessions-changelog".into(),
        );
        store.put("a".into(), 0, 10, 1).await;
        let at_cut = crate::store::api::StateStore::snapshot(&mut store).await;
        store.put("a".into(), 20, 30, 2).await;
        rewinds_to_its_cut(&mut store, &at_cut).await;
        check!(store.find_sessions(&"a".to_string(), 0, 100).await == vec![(0, 10, 1)]);
    }

    #[tokio::test]
    async fn join_window_store_rewinds_to_its_cut_with_its_seqnum() {
        use crate::store::join_window::{JoinWindowBytesStore, JoinWindowStore};

        let mut store = JoinWindowBytesStore::<String, i64>::in_memory(
            "joins".into(),
            string_serde(),
            i64_serde(),
            "joins-changelog".into(),
        );
        store.put("a".into(), 10, 1).await;
        let at_cut = crate::store::api::StateStore::snapshot(&mut store).await;
        store.put("a".into(), 10, 2).await;
        rewinds_to_its_cut(&mut store, &at_cut).await;
        // The duplicate at the same timestamp is gone, and the next put reuses
        // the sequence number the cut recorded.
        check!(store.fetch(&"a".to_string(), 0, 100).await == vec![(10, 1)]);
        store.put("a".into(), 10, 3).await;
        check!(store.fetch(&"a".to_string(), 0, 100).await == vec![(10, 1), (10, 3)]);
    }

    #[tokio::test]
    async fn versioned_store_rewinds_to_its_cut_with_its_stream_time() {
        use krabka_units::prelude::*;

        use crate::store::versioned::{VersionedBytesStore, VersionedKeyValueStore};

        let mut store = VersionedBytesStore::<String, i64>::in_memory(
            "history".into(),
            secs(1_000),
            string_serde(),
            i64_serde(),
            "history-changelog".into(),
        );
        store.put("a".into(), Some(1), 10).await;
        let at_cut = crate::store::api::StateStore::snapshot(&mut store).await;
        store.put("a".into(), Some(2), 20).await;
        rewinds_to_its_cut(&mut store, &at_cut).await;
        check!(store.get(&"a".to_string()).await.map(|r| r.value) == Some(1));
        // The observed stream time came back with the chains, so a version below
        // the restored horizon is still placed by the same rule.
        check!(store.get_as_of(&"a".to_string(), 20).await.map(|r| r.value) == Some(1));
    }

    #[tokio::test]
    async fn suppress_store_rewinds_to_its_cut_with_its_counters() {
        use crate::{
            dsl::processors::change::Change,
            store::{
                suppress_bufval::SuppressRecordCtx,
                suppress_store::{SuppressBytesStore, SuppressStore},
            },
        };

        let context = |timestamp| SuppressRecordCtx {
            topic: "in".to_owned(),
            partition: 0,
            offset: 0,
            timestamp,
        };
        let mut store = SuppressBytesStore::<String, i64>::in_memory(
            "buffer".into(),
            string_serde(),
            i64_serde(),
            "buffer-changelog".into(),
        );
        store
            .put(
                "a".into(),
                10,
                Change {
                    old: None,
                    new: Some(1),
                },
                context(10),
            )
            .await;
        let at_cut = crate::store::api::StateStore::snapshot(&mut store).await;
        let size_at_cut = store.byte_size();
        store
            .put(
                "b".into(),
                20,
                Change {
                    old: None,
                    new: Some(2),
                },
                context(20),
            )
            .await;
        rewinds_to_its_cut(&mut store, &at_cut).await;
        check!(store.len() == 1);
        check!(store.byte_size() == size_at_cut);
        let evicted = store.evict_while(100).await;
        check!(
            evicted
                == vec![(
                    "a".to_string(),
                    Change {
                        old: None,
                        new: Some(1)
                    },
                    10
                )]
        );
    }

    #[tokio::test]
    async fn join_grace_buffer_rewinds_to_its_cut_with_its_sequence() {
        use crate::store::join_grace_buffer::JoinGraceBufferStore;

        let mut store = JoinGraceBufferStore::<String, i64>::in_memory(
            "grace".into(),
            string_serde(),
            i64_serde(),
            "grace-changelog".into(),
        );
        store.put("a".into(), 1, 100).await;
        let at_cut = crate::store::api::StateStore::snapshot(&mut store).await;
        store.put("b".into(), 2, 200).await;
        rewinds_to_its_cut(&mut store, &at_cut).await;
        check!(store.len() == 1);
        // The restored sequence keeps a later put behind the recovered entry at
        // the same timestamp.
        store.put("c".into(), 3, 100).await;
        check!(
            store.drain_due(100).await
                == vec![("a".to_string(), 1, 100), ("c".to_string(), 3, 100)]
        );
    }

    #[tokio::test]
    async fn subscription_store_rewinds_to_its_cut() {
        use crate::{
            dsl::processors::fk::subscription::{Instruction, SubscriptionWrapper},
            store::fk_subscription::SubscriptionBytesStore,
        };

        let wrapper = |pk: &str| SubscriptionWrapper {
            instruction: Instruction::PropagateOnlyIfFkValAvailable,
            hash: Some(vec![7u8; 16]),
            primary_key: Bytes::copy_from_slice(pk.as_bytes()),
            primary_partition: 0,
        };
        let mut store = SubscriptionBytesStore::in_memory("subs".into(), "subs-changelog".into());
        store.put(b"FK1", b"pk1", &wrapper("pk1"), 10).await;
        let at_cut = crate::store::api::StateStore::snapshot(&mut store).await;
        store.put(b"FK1", b"pk2", &wrapper("pk2"), 11).await;
        rewinds_to_its_cut(&mut store, &at_cut).await;
        check!(store.range_by_foreign(b"FK1").await.len() == 1);
    }

    /// A store whose payload is malformed rejects the restore instead of
    /// half-applying it.
    #[tokio::test]
    async fn a_malformed_payload_is_rejected() {
        use crate::store::{api::StateStore, kv::KeyValueBytesStore};

        let mut store = KeyValueBytesStore::<String, i64>::in_memory(
            "counts".into(),
            string_serde(),
            i64_serde(),
            "counts-changelog".into(),
        );
        assert!(let Err(error) = store.restore_snapshot(Bytes::from_static(b"\x00")).await);
        check!(error.to_string().contains("truncated state snapshot"));
    }
}
