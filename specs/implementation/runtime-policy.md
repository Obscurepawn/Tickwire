# Order submission policy

One event-loop thread owns the policy state and the current grant through `Rc<RefCell<_>>`. A failed clock sample, backward reading or change of clock domain closes the current owner. The production clock comes from `tickwire-clock-sys`.

Configuration bounds the state retained in memory. The upper limits are 1,024 strategies, 256 market streams, 1,024 publication records per stream and eight required streams per strategy. Expiring an old publication also expires intent references to it. Lease revocation and changes of strategy generation are monotonic.

The execution observer acknowledges each ordered record on its authenticated market subscription. An unavailable observation revokes trading for the current feed and ring generation. The cursor continues through the trailing stale quote and later complete quotes on that generation. Only a live mark from a newer feed or ring generation can qualify a new trigger. A backward sequence, regressed generation or mismatched instrument version still rejects the observation.

A grant names the writer and strategy, the required market streams, and the action allowed. New exposure also requires a reconciled private source with a current `GapToken` and a continuous input WAL prefix. The exchange adapter owns signature validity and quota at send time. The account gateway owns durable admission and reservations.
