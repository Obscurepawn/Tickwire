# State and transition boundaries

`Reserve` binds an order to its original digest and budget. `PersistLocal` stands for completed local synchronization. `AcceptCommit` matches the supplied digest to the reserved order. The model treats A and B as distinct full records. The Rust implementation compares the entire `DurableRecord`, including its sequence.

`BeginSend` changes the state to unknown before the transport can accept bytes. `Ack` confirms the send, `Uncertain` freezes new admissions, and `NoBytes` releases an eligible reservation. An invalid zero-byte report freezes the owner. The environment supplies the gate value and the transport outcome. The [source anchors](02_models/anchors.lock) pin the reducer lines represented by these transitions.
