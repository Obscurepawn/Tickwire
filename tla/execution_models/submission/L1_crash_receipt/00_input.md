# Local journal and lost receipt

A submitter reserves an intent, stores its immutable dependency, commits a journal record and presents the returned receipt. The model explores a dependency failure, a failed append and a completed append whose reply is lost. A crash can occur between submission steps. Restart reads the journal into a frozen account. [Model](02_models/L1_CrashReceipt.tla) · [Mock services](../../../../crates/tickwire-mock/src/lib.rs)
