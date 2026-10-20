//! Content addressed local objects, writer exclusion and crash-recoverable CAS.
//! Files are synced before references become visible. A durable intent is the
//! only recovery authority; directory listings never select a dataset.
use crate::{Result, fail, sha256};
use fs4::fs_std::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
};
const MAX_OBJECT: u64 = 512 * 1024 * 1024;
const MAX_MANIFEST: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ObjectRef {
    pub path: String,
    pub sha256: String,
    pub bytes: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    parent: Option<String>,
    hash: String,
    manifest: Value,
}

pub struct Store {
    root: PathBuf,
    _lock: Option<File>,
    writer_operation: Mutex<()>,
    poisoned: AtomicBool,
    #[cfg(test)]
    directory_barrier: fn(&Path) -> Result<()>,
}
fn sync_dir(path: &Path) -> Result<()> {
    marketfeed_recording::durability::sync_directory(path)?;
    Ok(())
}
fn hash_valid(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn regular(path: &Path) -> Result<()> {
    let m = fs::symlink_metadata(path)?;
    if !m.is_file() || m.file_type().is_symlink() {
        return Err(fail("expected regular file"));
    }
    Ok(())
}
fn read_bounded(path: &Path, bound: u64) -> Result<Vec<u8>> {
    Ok(read_bounded_file(path, bound, false)?.1)
}
fn read_bounded_file(path: &Path, bound: u64, writable: bool) -> Result<(File, Vec<u8>)> {
    regular(path)?;
    let mut file = OpenOptions::new().read(true).write(writable).open(path)?;
    let mut bytes = Vec::new();
    (&mut file).take(bound + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > bound {
        return Err(fail("object exceeds resource bound"));
    }
    Ok((file, bytes))
}
impl Store {
    /// Locks remain at a stable inode. Never delete the lock file on close.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        if fs::symlink_metadata(&root)?.file_type().is_symlink() {
            return Err(fail("store root is a symlink"));
        }
        let lock_path = root.join("writer.lock");
        if fs::symlink_metadata(&lock_path).is_ok() {
            regular(&lock_path)?;
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        if !FileExt::try_lock_exclusive(&lock)? {
            return Err(fail("dataset store already has a writer"));
        }
        for name in ["objects", "manifests"] {
            let path = root.join(name);
            fs::create_dir_all(&path)?;
            if fs::symlink_metadata(&path)?.file_type().is_symlink() {
                return Err(fail("store directory is a symlink"));
            }
            sync_dir(&path)?;
        }
        marketfeed_recording::durability::sync_directory_ancestry(&root)?;
        Ok(Self {
            root,
            _lock: Some(lock),
            writer_operation: Mutex::new(()),
            poisoned: AtomicBool::new(false),
            #[cfg(test)]
            directory_barrier: sync_dir,
        })
    }
    /// Read an existing immutable publication without creating files, requiring
    /// write access, taking the writer lock, syncing or recovering anything.
    /// Pinned manifests and objects are hash checked on every read. Concurrent
    /// publication can advance HEAD atomically without changing pinned bytes.
    pub fn open_read_only(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        for path in [root.clone(), root.join("objects"), root.join("manifests")] {
            let metadata = fs::symlink_metadata(path)?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(fail("existing regular store directories required"));
            }
        }
        Ok(Self {
            root,
            _lock: None,
            writer_operation: Mutex::new(()),
            poisoned: AtomicBool::new(false),
            #[cfg(test)]
            directory_barrier: sync_dir,
        })
    }
    fn require_writer(&self) -> Result<()> {
        if self._lock.is_none() {
            return Err(fail(
                "read-only dataset store cannot mutate publication state",
            ));
        }
        if self.poisoned.load(Ordering::Acquire) {
            return Err(fail(
                "dataset store poisoned; reopen and recover before writing",
            ));
        }
        Ok(())
    }
    fn mutate<T>(&self, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        self.require_writer()?;
        let result = operation();
        if result.is_err() {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }
    fn writer_guard(&self) -> Result<MutexGuard<'_, ()>> {
        let guard = self
            .writer_operation
            .lock()
            .map_err(|_| fail("dataset writer operation panicked; reopen and recover"))?;
        // Check poison only after obtaining the lock: a previous operation may
        // have failed while this caller was waiting. Private mutation helpers
        // run under this guard and must not acquire it recursively.
        self.require_writer()?;
        Ok(guard)
    }
    fn sync_directory(&self, path: &Path) -> Result<()> {
        #[cfg(test)]
        {
            (self.directory_barrier)(path)
        }
        #[cfg(not(test))]
        {
            sync_dir(path)
        }
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn put(&self, bytes: &[u8]) -> Result<ObjectRef> {
        let _guard = self.writer_guard()?;
        if bytes.len() as u64 > MAX_OBJECT {
            return Err(fail("object exceeds resource bound"));
        }
        let hash = sha256(bytes);
        let relative = format!("objects/{hash}");
        self.immutable(&relative, bytes)?;
        Ok(ObjectRef {
            path: relative,
            sha256: hash,
            bytes: bytes.len().to_string(),
        })
    }
    fn immutable(&self, relative: &str, bytes: &[u8]) -> Result<()> {
        self.mutate(|| self.immutable_inner(relative, bytes))
    }
    fn immutable_inner(&self, relative: &str, bytes: &[u8]) -> Result<()> {
        let path = self.root.join(relative);
        if path.exists() {
            let (file, existing) = read_bounded_file(&path, MAX_OBJECT, true)?;
            if existing != bytes {
                return Err(fail("immutable object collision or corruption"));
            }
            // Equality alone cannot establish durability after an earlier
            // failed install, including a failure in a previous Store instance.
            file.sync_all()?;
            self.sync_directory(path.parent().ok_or_else(|| fail("object parent"))?)?;
            return Ok(());
        }
        // A leftover staging file is safe to replace only while holding the
        // persistent OS lock. It never serves as an admission reference.
        let temp = path.with_extension("staging");
        if temp.exists() {
            regular(&temp)?;
            fs::remove_file(&temp)?;
        }
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        drop(f);
        fs::hard_link(&temp, &path)?;
        self.sync_directory(path.parent().ok_or_else(|| fail("object parent"))?)?;
        fs::remove_file(temp)?;
        self.sync_directory(path.parent().ok_or_else(|| fail("object parent"))?)?;
        Ok(())
    }
    pub fn object(&self, reference: &ObjectRef) -> Result<Vec<u8>> {
        self.checked_object(reference, false)
    }
    fn checked_object(&self, reference: &ObjectRef, durable: bool) -> Result<Vec<u8>> {
        if !hash_valid(&reference.sha256)
            || reference.path != format!("objects/{}", reference.sha256)
        {
            return Err(fail("noncanonical object reference"));
        }
        let size: u64 = reference.bytes.parse()?;
        if size > MAX_OBJECT || size.to_string() != reference.bytes {
            return Err(fail("invalid object byte count"));
        }
        let path = self.root.join(&reference.path);
        let (file, bytes) = read_bounded_file(&path, size, durable)?;
        if bytes.len() as u64 != size || sha256(&bytes) != reference.sha256 {
            return Err(fail("object hash or size mismatch"));
        }
        if durable {
            // Writer admission verifies and syncs the same opened file. Public
            // and read-only checks never request write access or sync anything.
            self.require_writer()?;
            file.sync_all()?;
            self.sync_directory(path.parent().ok_or_else(|| fail("object parent"))?)?;
        }
        Ok(bytes)
    }
    pub fn head(&self) -> Result<Option<String>> {
        let path = self.root.join("HEAD.json");
        if !path.exists() {
            return Ok(None);
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Head {
            manifest_sha256: String,
        }
        let h: Head = serde_json::from_slice(&read_bounded(&path, 4096)?)?;
        if !hash_valid(&h.manifest_sha256) {
            return Err(fail("invalid manifest head"));
        }
        Ok(Some(h.manifest_sha256))
    }
    pub fn manifest(&self, hash: &str) -> Result<Value> {
        if !hash_valid(hash) {
            return Err(fail("invalid manifest pin"));
        }
        let bytes = read_bounded(
            &self.root.join(format!("manifests/{hash}.json")),
            MAX_MANIFEST,
        )?;
        if sha256(&bytes) != hash {
            return Err(fail("manifest hash mismatch"));
        }
        Ok(serde_json::from_slice(&bytes)?)
    }
    /// Every embedded object reference is canonical, bounded and hash checked.
    pub fn verify_references(&self, value: &Value) -> Result<()> {
        self.verify_references_mode(value, false)
    }
    fn verify_references_mode(&self, value: &Value, durable: bool) -> Result<()> {
        fn walk(
            store: &Store,
            value: &Value,
            seen: &mut std::collections::BTreeMap<String, ObjectRef>,
            bytes: &mut u64,
            nodes: &mut usize,
            depth: usize,
            durable: bool,
        ) -> Result<()> {
            *nodes += 1;
            if *nodes > 100_000 || depth > 32 {
                return Err(fail("manifest traversal budget"));
            }
            match value {
                Value::Object(map)
                    if map.contains_key("path")
                        && map.contains_key("sha256")
                        && map.contains_key("bytes") =>
                {
                    let reference: ObjectRef = serde_json::from_value(value.clone())?;
                    if let Some(prior) = seen.get(&reference.sha256) {
                        if prior != &reference {
                            return Err(fail("inconsistent repeated object reference"));
                        }
                    } else {
                        *bytes = bytes
                            .checked_add(reference.bytes.parse::<u64>()?)
                            .ok_or_else(|| fail("object byte overflow"))?;
                        if *bytes > MAX_OBJECT || seen.len() >= 4096 {
                            return Err(fail("dataset object work budget"));
                        }
                        store.checked_object(&reference, durable)?;
                        seen.insert(reference.sha256.clone(), reference);
                    }
                }
                Value::Object(map) => {
                    for v in map.values() {
                        walk(store, v, seen, bytes, nodes, depth + 1, durable)?;
                    }
                }
                Value::Array(values) => {
                    for v in values {
                        walk(store, v, seen, bytes, nodes, depth + 1, durable)?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
        walk(
            self,
            value,
            &mut std::collections::BTreeMap::new(),
            &mut 0,
            &mut 0,
            0,
            durable,
        )
    }
    pub fn publish(&self, manifest: Value, expected_parent: Option<&str>) -> Result<String> {
        let _guard = self.writer_guard()?;
        self.mutate(|| self.recover_inner())?;
        if self.head()?.as_deref() != expected_parent {
            return Err(fail("manifest parent CAS failed"));
        }
        if manifest["parent_manifest_sha256"].as_str() != expected_parent {
            return Err(fail("manifest parent does not match CAS"));
        }
        self.mutate(|| self.verify_references_mode(&manifest, true))?;
        let bytes = serde_json::to_vec(&manifest)?;
        if bytes.len() as u64 > MAX_MANIFEST {
            return Err(fail("manifest too large"));
        }
        let hash = sha256(&bytes);
        let intent = Intent {
            parent: expected_parent.map(str::to_owned),
            hash: hash.clone(),
            manifest,
        };
        self.write_intent(&intent)?;
        self.mutate(|| self.recover_inner())?;
        Ok(hash)
    }
    fn write_intent(&self, intent: &Intent) -> Result<()> {
        self.mutate(|| self.write_intent_inner(intent))
    }
    fn write_intent_inner(&self, intent: &Intent) -> Result<()> {
        let bytes = serde_json::to_vec(intent)?;
        if bytes.len() as u64 > MAX_MANIFEST + 4096 {
            return Err(fail("publication intent too large"));
        }
        let path = self.root.join("publication.intent.json");
        if fs::symlink_metadata(&path).is_ok() {
            return Err(fail("publication intent already exists"));
        }
        let temp = self.root.join("publication.intent.staging");
        if fs::symlink_metadata(&temp).is_ok() {
            regular(&temp)?;
            fs::remove_file(&temp)?;
        }
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
        drop(f);
        fs::hard_link(&temp, path)?;
        self.sync_directory(&self.root)?;
        fs::remove_file(temp)?;
        self.sync_directory(&self.root)
    }
    pub fn recover(&self) -> Result<Option<String>> {
        let _guard = self.writer_guard()?;
        self.mutate(|| self.recover_inner())
    }
    fn recover_inner(&self) -> Result<Option<String>> {
        let path = self.root.join("publication.intent.json");
        if !path.exists() {
            return Ok(None);
        }
        let (intent_file, intent_bytes) = read_bounded_file(&path, MAX_MANIFEST + 4096, true)?;
        let intent: Intent = serde_json::from_slice(&intent_bytes)?;
        let bytes = serde_json::to_vec(&intent.manifest)?;
        if !hash_valid(&intent.hash)
            || sha256(&bytes) != intent.hash
            || intent.manifest["parent_manifest_sha256"].as_str() != intent.parent.as_deref()
        {
            return Err(fail("invalid durable publication intent"));
        }
        let head = self.head()?;
        if head != intent.parent && head.as_deref() != Some(&intent.hash) {
            return Err(fail("publication intent conflicts with head"));
        }
        intent_file.sync_all()?;
        self.sync_directory(&self.root)?;
        self.verify_references_mode(&intent.manifest, true)?;
        self.immutable(&format!("manifests/{}.json", intent.hash), &bytes)?;
        let temp = self.root.join("HEAD.tmp");
        if temp.exists() {
            regular(&temp)?;
            fs::remove_file(&temp)?;
        }
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        f.write_all(&serde_json::to_vec(
            &serde_json::json!({"manifest_sha256":intent.hash}),
        )?)?;
        f.sync_all()?;
        drop(f);
        fs::rename(temp, self.root.join("HEAD.json"))?;
        self.sync_directory(&self.root)?;
        fs::remove_file(path)?;
        self.sync_directory(&self.root)?;
        Ok(Some(intent.hash))
    }
    /// Pins must be reachable through committed ancestry. Prepared orphan
    /// manifests cannot masquerade as a completed publication.
    pub fn published_manifest(&self, pin: &str) -> Result<Value> {
        let mut current = self.head()?.ok_or_else(|| fail("no published head"))?;
        let mut seen = std::collections::BTreeSet::new();
        let mut manifest_bytes = 0usize;
        loop {
            if !seen.insert(current.clone()) || seen.len() > 1024 {
                return Err(fail("manifest ancestry cycle or limit"));
            }
            let manifest = self.manifest(&current)?;
            manifest_bytes += serde_json::to_vec(&manifest)?.len();
            if manifest_bytes > 32 * 1024 * 1024 {
                return Err(fail("manifest ancestry byte budget"));
            }
            if current == pin {
                self.verify_references(&manifest)?;
                return Ok(manifest);
            }
            current = manifest["parent_manifest_sha256"]
                .as_str()
                .ok_or_else(|| fail("pin not in published ancestry"))?
                .to_owned();
        }
    }
    /// A fresh destination is mandatory. Copies every manifest in the selected
    /// ancestry and every referenced object, then verifies before setting HEAD.
    pub fn backup_to(&self, pin: &str, destination: &Path) -> Result<()> {
        if destination.exists() {
            return Err(fail("backup/restore destination must be new"));
        }
        let mut manifest = self.published_manifest(pin)?;
        let backup = Store::open(destination)?;
        let mut hash = pin.to_owned();
        let mut copied = std::collections::BTreeMap::new();
        let mut copied_bytes = 0u64;
        let mut seen = std::collections::BTreeSet::new();
        let mut manifest_bytes = 0usize;
        loop {
            if !seen.insert(hash.clone()) || seen.len() > 1024 {
                return Err(fail("manifest ancestry cycle or limit"));
            }
            manifest_bytes += serde_json::to_vec(&manifest)?.len();
            if manifest_bytes > 32 * 1024 * 1024 {
                return Err(fail("backup ancestry byte budget"));
            }
            fn copy_refs(
                source: &Store,
                dest: &Store,
                v: &Value,
                copied: &mut std::collections::BTreeMap<String, ObjectRef>,
                copied_bytes: &mut u64,
            ) -> Result<()> {
                if let Some(m) = v.as_object() {
                    if m.contains_key("path") && m.contains_key("sha256") && m.contains_key("bytes")
                    {
                        let r: ObjectRef = serde_json::from_value(v.clone())?;
                        if let Some(previous) = copied.get(&r.sha256) {
                            if previous != &r {
                                return Err(fail("inconsistent repeated backup reference"));
                            }
                            return Ok(());
                        }
                        copied.insert(r.sha256.clone(), r.clone());
                        *copied_bytes = copied_bytes
                            .checked_add(r.bytes.parse::<u64>()?)
                            .ok_or_else(|| fail("backup byte overflow"))?;
                        if *copied_bytes > MAX_OBJECT || copied.len() > 4096 {
                            return Err(fail("backup object work budget"));
                        }
                        let b = source.object(&r)?;
                        if dest.put(&b)? != r {
                            return Err(fail("backup reference changed"));
                        }
                    } else {
                        for child in m.values() {
                            copy_refs(source, dest, child, copied, copied_bytes)?;
                        }
                    }
                } else if let Some(a) = v.as_array() {
                    for child in a {
                        copy_refs(source, dest, child, copied, copied_bytes)?;
                    }
                }
                Ok(())
            }
            copy_refs(self, &backup, &manifest, &mut copied, &mut copied_bytes)?;
            backup.immutable(
                &format!("manifests/{hash}.json"),
                &serde_json::to_vec(&manifest)?,
            )?;
            let Some(parent) = manifest["parent_manifest_sha256"].as_str() else {
                break;
            };
            hash = parent.to_owned();
            manifest = self.manifest(&hash)?;
        }
        // An intent makes interrupted finalization recoverable on the backup.
        let selected = self.manifest(pin)?;
        // The ancestor chain is verified above; initialize HEAD to the selected
        // parent, then use the same checked publication recovery path.
        if let Some(parent) = selected["parent_manifest_sha256"].as_str() {
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(destination.join("HEAD.json"))?;
            f.write_all(&serde_json::to_vec(
                &serde_json::json!({"manifest_sha256":parent}),
            )?)?;
            f.sync_all()?;
            sync_dir(destination)?;
        }
        backup.write_intent(&Intent {
            parent: selected["parent_manifest_sha256"]
                .as_str()
                .map(str::to_owned),
            hash: pin.into(),
            manifest: selected.clone(),
        })?;
        backup.recover()?;
        backup.published_manifest(pin)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fail_object_barrier(path: &Path) -> Result<()> {
        if path.file_name().is_some_and(|name| name == "objects") {
            return Err(fail("injected object namespace barrier failure"));
        }
        sync_dir(path)
    }
    fn fail_visible_head_barrier(path: &Path) -> Result<()> {
        if path.join("HEAD.json").exists() {
            return Err(fail("injected post-HEAD namespace barrier failure"));
        }
        sync_dir(path)
    }

    #[test]
    fn failed_object_install_poisoning_and_existing_object_reconciliation() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("store");
        let mut store = Store::open(&root).unwrap();
        let first = manifest(&store, None, "acknowledged");
        let pin = store.publish(first.clone(), None).unwrap();
        let bytes = b"installed-but-not-acknowledged";
        let reference = ObjectRef {
            path: format!("objects/{}", sha256(bytes)),
            sha256: sha256(bytes),
            bytes: bytes.len().to_string(),
        };
        store.directory_barrier = fail_object_barrier;
        assert!(
            store
                .put(bytes)
                .unwrap_err()
                .to_string()
                .contains("injected")
        );
        assert_eq!(fs::read(root.join(&reference.path)).unwrap(), bytes);
        let next = serde_json::json!({"parent_manifest_sha256":pin,"object":reference});
        for error in [
            store.put(bytes).unwrap_err(),
            store.publish(next.clone(), Some(&pin)).unwrap_err(),
            store.recover().unwrap_err(),
        ] {
            assert!(error.to_string().contains("poisoned"));
        }
        assert_eq!(store.head().unwrap(), Some(pin.clone()));
        let reader = Store::open_read_only(&root).unwrap();
        assert_eq!(reader.published_manifest(&pin).unwrap(), first);
        drop(store);

        // A reopened writer must sync even a matching existing object. Equality
        // cannot turn the previous failed namespace barrier into an ACK.
        let mut store = Store::open(&root).unwrap();
        store.directory_barrier = fail_object_barrier;
        assert!(
            store
                .put(bytes)
                .unwrap_err()
                .to_string()
                .contains("injected")
        );
        drop(store);
        let store = Store::open(&root).unwrap();
        assert_eq!(store.put(bytes).unwrap(), reference);
        let next_pin = store.publish(next.clone(), Some(&pin)).unwrap();
        assert_eq!(reader.published_manifest(&next_pin).unwrap(), next);
    }

    #[test]
    fn direct_reference_reuse_requires_barriers_in_publication_and_recovery() {
        for recovering in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("store");
            let store = Store::open(&root).unwrap();
            let value = manifest(&store, None, "existing-object");
            let hash = sha256(&serde_json::to_vec(&value).unwrap());
            if recovering {
                store
                    .write_intent(&Intent {
                        parent: None,
                        hash: hash.clone(),
                        manifest: value.clone(),
                    })
                    .unwrap();
            }
            drop(store);
            let mut store = Store::open(&root).unwrap();
            store.directory_barrier = fail_object_barrier;
            // Neither path calls put(): each must independently sync objects
            // referenced by a caller or a recovered intent before installing HEAD.
            let failure = if recovering {
                store.recover().unwrap_err()
            } else {
                store.publish(value.clone(), None).unwrap_err()
            };
            assert!(failure.to_string().contains("injected"));
            assert!(!root.join("HEAD.json").exists());
            assert_eq!(root.join("publication.intent.json").exists(), recovering);
            assert!(
                store
                    .recover()
                    .unwrap_err()
                    .to_string()
                    .contains("poisoned")
            );
            drop(store);
            let store = Store::open(&root).unwrap();
            if recovering {
                assert_eq!(store.recover().unwrap(), Some(hash.clone()));
            } else {
                assert_eq!(store.publish(value.clone(), None).unwrap(), hash);
            }
            assert_eq!(store.published_manifest(&hash).unwrap(), value);
        }
    }

    #[test]
    fn post_head_barrier_failure_requires_reopen_and_intent_recovery() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("store");
        let mut store = Store::open(&root).unwrap();
        let value = manifest(&store, None, "recoverable");
        let hash = sha256(&serde_json::to_vec(&value).unwrap());
        store
            .write_intent(&Intent {
                parent: None,
                hash: hash.clone(),
                manifest: value.clone(),
            })
            .unwrap();
        store.directory_barrier = fail_visible_head_barrier;
        assert!(
            store
                .recover()
                .unwrap_err()
                .to_string()
                .contains("post-HEAD")
        );
        assert_eq!(store.head().unwrap(), Some(hash.clone()));
        assert!(root.join("publication.intent.json").exists());
        assert!(
            store
                .recover()
                .unwrap_err()
                .to_string()
                .contains("poisoned")
        );
        drop(store);
        let store = Store::open(&root).unwrap();
        assert_eq!(store.recover().unwrap(), Some(hash.clone()));
        assert_eq!(store.published_manifest(&hash).unwrap(), value);
        assert!(!root.join("publication.intent.json").exists());
    }

    #[test]
    fn shared_store_serializes_same_object_writes_and_competing_parent_cas() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("nested/new/store")).unwrap();
        let ready = std::sync::Barrier::new(8);
        let results = std::thread::scope(|scope| {
            let jobs: Vec<_> = (0..8)
                .map(|candidate| {
                    let store = &store;
                    let ready = &ready;
                    scope.spawn(move || {
                        ready.wait();
                        let reference = store.put(&vec![7; 32_768]).unwrap();
                        let value = serde_json::json!({
                            "parent_manifest_sha256":null,
                            "candidate":candidate,
                            "object":reference,
                        });
                        store.publish(value, None)
                    })
                })
                .collect();
            jobs.into_iter()
                .map(|job| job.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        for result in results.into_iter().filter(|result| result.is_err()) {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("parent CAS failed")
            );
        }
        let pin = store.head().unwrap().unwrap();
        store.published_manifest(&pin).unwrap();
        // A rejected stale CAS is a validation refusal, not a storage failure.
        store.put(b"still-healthy").unwrap();
        assert!(store.recover().unwrap().is_none());
    }

    #[test]
    fn read_only_store_never_creates_or_recovers_and_does_not_take_writer_lock() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("missing");
        assert!(Store::open_read_only(&missing).is_err());
        assert!(!missing.exists());
        let incomplete = temp.path().join("incomplete");
        fs::create_dir(&incomplete).unwrap();
        assert!(Store::open_read_only(&incomplete).is_err());
        assert_eq!(fs::read_dir(&incomplete).unwrap().count(), 0);
        let root = temp.path().join("complete");
        let writer = Store::open(&root).unwrap();
        let first = manifest(&writer, None, "published");
        let pin = writer.publish(first.clone(), None).unwrap();
        let reader = Store::open_read_only(&root).unwrap();
        assert_eq!(reader.published_manifest(&pin).unwrap(), first);
        assert!(reader.put(b"must-not-exist").is_err());
        assert!(reader.publish(serde_json::json!({}), Some(&pin)).is_err());
        assert!(reader.recover().is_err());
        assert!(!root.join("publication.intent.json").exists());
        drop(writer);
        fs::remove_file(root.join("writer.lock")).unwrap();
        let reader = Store::open_read_only(&root).unwrap();
        assert_eq!(reader.published_manifest(&pin).unwrap(), first);
        assert!(!root.join("writer.lock").exists());
    }
    #[test]
    fn interrupted_intent_staging_never_becomes_a_recovery_authority() {
        for bytes in [
            b"".as_slice(),
            b"{\"parent\":".as_slice(),
            b"{\"not-yet-synced\":true}".as_slice(),
        ] {
            let temp = tempfile::tempdir().unwrap();
            {
                let store = Store::open(temp.path()).unwrap();
                fs::write(store.root.join("publication.intent.staging"), bytes).unwrap();
            }
            let store = Store::open(temp.path()).unwrap();
            assert!(store.recover().unwrap().is_none());
            let pin = store
                .publish(manifest(&store, None, "valid"), None)
                .unwrap();
            assert!(store.published_manifest(&pin).is_ok());
        }
    }
    fn manifest(store: &Store, parent: Option<&str>, data: &str) -> Value {
        serde_json::json!({"schema_version":1,"parent_manifest_sha256":parent,"object":store.put(data.as_bytes()).unwrap()})
    }
    #[test]
    fn locking_cas_corruption_and_immutable_objects() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path()).unwrap();
        assert!(Store::open(temp.path()).is_err());
        let first = manifest(&store, None, "first");
        let pin = store.publish(first.clone(), None).unwrap();
        assert_eq!(store.published_manifest(&pin).unwrap(), first);
        assert!(
            store
                .publish(manifest(&store, None, "stale-parent"), None)
                .is_err()
        );
        let second = manifest(&store, Some(&pin), "second");
        let pin2 = store.publish(second, Some(&pin)).unwrap();
        assert!(store.published_manifest(&pin).is_ok());
        assert_eq!(store.head().unwrap().as_deref(), Some(pin2.as_str()));
        let reference = store.put(b"tamper-target").unwrap();
        fs::write(store.root.join(&reference.path), b"bad").unwrap();
        assert!(store.object(&reference).is_err());
        assert!(store.put(b"tamper-target").is_err());
    }
    #[test]
    fn recovery_at_intent_manifest_and_head_cutpoints_is_idempotent() {
        for cut in 0..3 {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("store");
            let store = Store::open(&root).unwrap();
            let first = store
                .publish(manifest(&store, None, "first"), None)
                .unwrap();
            let value = manifest(&store, Some(&first), "second");
            let hash = sha256(&serde_json::to_vec(&value).unwrap());
            store
                .write_intent(&Intent {
                    parent: Some(first.clone()),
                    hash: hash.clone(),
                    manifest: value.clone(),
                })
                .unwrap();
            if cut >= 1 {
                store
                    .immutable(
                        &format!("manifests/{hash}.json"),
                        &serde_json::to_vec(&value).unwrap(),
                    )
                    .unwrap();
            }
            if cut >= 2 {
                fs::write(
                    root.join("HEAD.json"),
                    serde_json::to_vec(&serde_json::json!({"manifest_sha256":hash})).unwrap(),
                )
                .unwrap();
            }
            drop(store);
            let store = Store::open(&root).unwrap();
            assert_eq!(store.recover().unwrap(), Some(hash.clone()));
            assert!(store.recover().unwrap().is_none());
            assert_eq!(store.published_manifest(&hash).unwrap(), value);
            assert!(store.published_manifest(&first).is_ok());
        }
    }
    #[test]
    fn backup_restores_complete_ancestry_and_refuses_tampering_or_reuse() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("source")).unwrap();
        let first = store
            .publish(manifest(&store, None, "first"), None)
            .unwrap();
        let second = store
            .publish(manifest(&store, Some(&first), "second"), Some(&first))
            .unwrap();
        let path = temp.path().join("backup");
        store.backup_to(&second, &path).unwrap();
        assert!(store.backup_to(&second, &path).is_err());
        let backup = Store::open(&path).unwrap();
        assert!(backup.published_manifest(&first).is_ok());
        let restored = temp.path().join("restored");
        backup.backup_to(&second, &restored).unwrap();
        let restore = Store::open(restored).unwrap();
        assert_eq!(
            restore.published_manifest(&second).unwrap(),
            store.published_manifest(&second).unwrap()
        );
        let r = store.put(b"second").unwrap();
        fs::write(path.join(&r.path), b"tampered").unwrap();
        assert!(
            backup
                .backup_to(&second, &temp.path().join("must-fail"))
                .is_err()
        );
    }
    #[cfg(unix)]
    #[test]
    fn references_and_locks_cannot_escape_via_symlinks() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("store");
        fs::create_dir(&root).unwrap();
        let outside = temp.path().join("outside");
        std::os::unix::fs::symlink(&outside, root.join("writer.lock")).unwrap();
        assert!(Store::open(&root).is_err());
        assert!(!outside.exists());
        fs::remove_file(root.join("writer.lock")).unwrap();
        let store = Store::open(&root).unwrap();
        let mut reference = store.put(b"value").unwrap();
        reference.path = "../outside".into();
        assert!(store.object(&reference).is_err());
    }
}
