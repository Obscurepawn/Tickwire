# A foreign cancel stops a ready peer

The deterministic [TLC trace](evidence/PeerCanContinue.log) uses two cash units, no crash, positive permissions and both orders' correct client IDs. Member 2 is scheduled first. Member 1's second descriptor cancels its own order. Member 2's second descriptor attempts to cancel member 1's order.

| State | Action | Reservations | Observable result |
| --- | --- | ---: | --- |
| 1 | Init | 0 | Both queues start at one |
| 2 | Admit member 2 buy | 1 | Local cursor 2 advances |
| 3 | Send | 1 | One native buy from member 2 |
| 4 | Ack | 1 | Member 2 gets its reply |
| 5 | Admit member 1 buy | 2 | Local cursor 1 advances |
| 6 | Send | 2 | One native buy from member 1 |
| 7 | Ack | 2 | Member 1 gets its reply and has a valid own-order cancel ready |
| 8 | RejectTarget | 2 | Member 2's cross-owner cancel fails and the live account process exits |

The witness requires a registered member to know the target order's original identifiers. The order lookup succeeds. Account management authorization rejects the command before booking or first write. The live loop then propagates this ordinary pre-admission identity error, leaving the other member's actual pending cancellation unserved. No unauthorized cancellation or release occurred. [Independent review and contract](06_review.md)

## Native reproduction on Bybit and OKX

The repository’s loopback TLS exchange fixture reproduced the same failure in independent Bybit and OKX `tickwire-spot-runner live` processes. The fixture pauses its quote publisher until the account reports that its market view is stale. It publishes one fresh quote and waits for the account to report a live view before it exposes the buy referencing that quote. On each venue, member 8 placed buy 801 and received its ACK. Before the exchange sent that ACK, the fixture published two descriptors for the same open order. Member 9’s cross-owner cancel 901 entered the scheduler first. The process returned `Session(Gateway(Account(Identity)))` and exited with status 1 while member 8’s own cancel 802 remained in the queue. The exchange accepted one signed buy on each venue. The native trace needs only that one buy to expose the peer shutdown. [Process results](evidence/native/run.log)

Run `python3 tla/execution_models/strategy_intake/L0_shared_cash/evidence/native/run.py --repeat 3` on Linux with the repository’s offline Cargo dependencies installed. The probe builds the production executable at the checkout and runs it against a local exchange. It copies the existing integration fixture into a temporary crate and configures two registered strategy queues and their grants. [Probe runner](evidence/native/run.py) · [Fixture changes](evidence/native/two_queues.patch)
