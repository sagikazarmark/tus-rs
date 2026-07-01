# Allow breaking storage handle persistence

Storage handle persistence may change the persisted upload state format, even if existing stored uploads that rely on the old `storage_key` and storage-internal facts become unreadable. We accept this compatibility break to deepen the storage handle persistence module: storage-specific facts should have locality behind the storage/state seam instead of leaking through public upload state.
