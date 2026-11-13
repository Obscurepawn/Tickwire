use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tickwire_recovery::{
    AsyncFileBlobStore, BlobStore, FileBlobReader, InMemoryBlobStore, RuntimeBlobStore, StoreKey,
    digest,
};

fn temp_dir(tag: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "tickwire-recovery-modes-{tag}-{}-{n}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.parent().unwrap()).unwrap();
    dir
}

#[test]
fn in_memory_and_async_file_blob_stores_deduplicate_detect_conflicts_and_persist() {
    // 1. InMemoryBlobStore
    let mut mem = InMemoryBlobStore::new();
    let obj = b"strategy-elf-pin-bytes";
    let key = StoreKey::Object(digest(obj));
    mem.put_sync(key, obj).unwrap();
    mem.put_sync(key, obj).unwrap(); // idempotent retry
    assert_eq!(mem.get(key, 1024).unwrap(), obj);
    assert!(mem.put_sync(key, b"different-bytes").is_err()); // conflict

    // 2. AsyncFileBlobStore + FileBlobReader verification
    let dir = temp_dir("async-blobs");
    {
        let mut async_store = AsyncFileBlobStore::open(&dir).unwrap();
        async_store.put_sync(key, obj).unwrap();
        // Second put hits RAM index with zero disk I/O
        async_store.put_sync(key, obj).unwrap();
        assert_eq!(async_store.get(key, 1024).unwrap(), obj);
        assert!(async_store.put_sync(key, b"mutated").is_err());

        let bind_key = StoreKey::Binding([0x77; 32]);
        let manifest_hash = [0x88; 32];
        async_store.put_sync(bind_key, &manifest_hash).unwrap();
        async_store.flush_durable().unwrap();
    }

    // Reopen via RuntimeBlobStore::mmap_async and verify pre-existing files loaded into index
    {
        let mut reopened = RuntimeBlobStore::mmap_async(&dir).unwrap();
        assert_eq!(reopened.get(key, 1024).unwrap(), obj);
        reopened.put_sync(key, obj).unwrap();
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let uid = fs::metadata(&dir).unwrap().uid();
        let reader = FileBlobReader::open(&dir, uid).unwrap();
        assert_eq!(reader.get_object(digest(obj), 1024).unwrap(), obj);
    }

    let _ = fs::remove_dir_all(&dir);
}
