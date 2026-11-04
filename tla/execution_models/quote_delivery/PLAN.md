# Shared quote delivery

One topic owner publishes complete BBO batches. The strategy worker and account process consume independent SHM cursors. L0 follows the records into the strategy callback, the account market mark and the subscription failure path. A subsequent level can add multi-instrument cutover while the account consumes catch-up records. [Source and assumptions](L0_pair/00_input.md)
