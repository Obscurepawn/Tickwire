--------------------------- MODULE L0_BinanceSpot ---------------------------
EXTENDS Naturals
CONSTANTS Credit, Deliveries, Faults
(*
 One eligible Binance depth trigger, one hosted worker, one spot limit buy with zero fee.
 A previously qualified account is the initial cut. Time calibration is reacquired here.
 pc: CoreNode.step_inner orders clock, market, private, account I/O/reports, strategies.
   crates/tickwire-runtime/src/node.rs:396-413
 md.pub, md.mark, md.intent: publisher, execution replica, worker IPC submission, respectively.
   crates/tickwire-app/src/main.rs:154-174 crates/tickwire-app/src/main.rs:207-230
   crates/tickwire-market-source/src/shm.rs:287-307 crates/tickwire-strategy-sdk/src/worker.rs:699-733
 window: this command's still-open time/lease horizon. calibrated: signed reading available.
   crates/tickwire-runtime/src/node.rs:415-441 crates/tickwire-binance-execution/src/lib.rs:406-414
 gap: remote input pending versus locally observed gap. The order gate uses the local observation.
   crates/tickwire-runtime/src/node.rs:501-549 crates/tickwire-runtime-policy/src/lib.rs:900-925
 tx.reg, tx.closed, tx.wal: input registration, durable recovery closure, durable admission.
   crates/tickwire-binance-account/src/lib.rs:377-394 crates/tickwire-runtime/src/authority.rs:146-211
   crates/tickwire-account-gateway/src/lib.rs:448-506
 tx.state: single outstanding request and volatile output progress. tx.writes: independent
 ghost count at full native acceptance. permit: remaining command quota in one quota window.
   crates/tickwire-binance-account/src/rest.rs:112-143 crates/tickwire-binance-execution/src/lib.rs:562-581
 input: number of durable authenticated execution reports (identical trade ID). applied: consumed prefix.
   crates/tickwire-binance-account/src/runtime.rs:125-175
 ledger.quote, ledger.base: normalized zero-fee asset totals, initially one unit cash / zero base.
 ledger.booked: whether the economic trade identity has been booked, independent from source identity.
   crates/tickwire-account/src/events.rs:56-113 crates/tickwire-account/src/events.rs:187-250
 reports.host, reports.child: reporting of this trade's durable account record across the worker boundary.
   crates/tickwire-runtime/src/node/strategy.rs:52-71 crates/tickwire-strategy-host/src/lib.rs:921-940
*)
VARIABLES pc, md, calibrated, window, gap, tx, permit, input, applied, ledger, reports
vars == <<pc,md,calibrated,window,gap,tx,permit,input,applied,ledger,reports>>
Init ==
    /\ pc = "clock" /\ md = [pub |-> FALSE, mark |-> FALSE, intent |-> FALSE]
    /\ calibrated = FALSE /\ window = TRUE /\ gap = "none" /\ permit = Credit
    /\ tx = [reg |-> FALSE, closed |-> FALSE, wal |-> FALSE, state |-> "idle",
             writes |-> 0, writeAuthorized |-> FALSE, ack |-> FALSE]
    /\ input = 0 /\ applied = 0 /\ ledger = [quote |-> 1, base |-> 0, booked |-> FALSE]
    /\ reports = [host |-> FALSE, child |-> FALSE]

\* The eligible price is injected by the peer only after startup. MD publishes the covered
\* depth observation and the registered worker may consume it on its independent cursor.
\* crates/tickwire-lab/src/launch.rs:99-121 crates/tickwire-strategy-sdk/src/worker.rs:699-733
Publish == /\ ~md.pub /\ md' = [md EXCEPT !.pub = TRUE]
           /\ UNCHANGED <<pc,calibrated,window,gap,tx,permit,input,applied,ledger,reports>>
WorkerDecide == /\ md.pub /\ ~md.intent /\ md' = [md EXCEPT !.intent = TRUE]
                /\ UNCHANGED <<pc,calibrated,window,gap,tx,permit,input,applied,ledger,reports>>
\* Monotonic time can cross the command/lease horizon between intake and first write.
\* crates/tickwire-binance-execution/src/lib.rs:562-580
Expire == /\ Faults /\ window /\ window' = FALSE
          /\ UNCHANGED <<pc,md,calibrated,gap,tx,permit,input,applied,ledger,reports>>
\* Socket loss may precede local observation. Gap is processed only during poll_private.
\* crates/tickwire-runtime/src/node.rs:539-549 crates/tickwire-binance-account/src/rest.rs:311-316
LoseSocket == /\ Faults /\ gap = "none" /\ gap' = "pending"
              /\ UNCHANGED <<pc,md,calibrated,window,tx,permit,input,applied,ledger,reports>>
\* The worker consumes the report on its own turn, in sequence after previous records.
\* crates/tickwire-strategy-sdk/src/worker.rs:576-605
WorkerReport == /\ reports.host /\ ~reports.child /\ reports' = [reports EXCEPT !.child = TRUE]
                /\ UNCHANGED <<pc,md,calibrated,window,gap,tx,permit,input,applied,ledger>>

\* A sample or an in-flight calibration may yield before the next turn.
\* crates/tickwire-runtime/src/node.rs:415-441
ClockPoll == /\ pc = "clock" /\ calibrated' \in {calibrated, TRUE} /\ pc' = "market"
             /\ UNCHANGED <<md,window,gap,tx,permit,input,applied,ledger,reports>>
\* External market input registers a mark in the account policy; no strategy callback dispatch here.
\* crates/tickwire-runtime/src/node.rs:145-152 crates/tickwire-runtime/src/node.rs:446-468
MarketPoll == /\ pc = "market" /\ md' = [md EXCEPT !.mark = md.pub] /\ pc' = "private"
              /\ UNCHANGED <<calibrated,window,gap,tx,permit,input,applied,ledger,reports>>
ObserveGap == /\ pc = "private" /\ gap = "pending" /\ gap' = "observed" /\ pc' = "rest"
              /\ UNCHANGED <<md,calibrated,window,tx,permit,input,applied,ledger,reports>>
\* An authenticated executionReport for this exact sent order is appended before account mutation.
\* crates/tickwire-binance-account/src/runtime.rs:125-175 crates/tickwire-lab/src/fixture.rs:546-557
CaptureFill == /\ pc = "private" /\ gap # "observed" /\ tx.writes > 0 /\ input < Deliveries
               /\ input' = input + 1 /\ pc' = "apply"
               /\ UNCHANGED <<md,calibrated,window,gap,tx,permit,applied,ledger,reports>>
PrivateIdle == /\ pc = "private" /\ pc' = "rest"
               /\ UNCHANGED <<md,calibrated,window,gap,tx,permit,input,applied,ledger,reports>>
\* Repeat delivery has another durable input position but the same immutable economics/trade ID.
\* crates/tickwire-account/src/events.rs:56-113 crates/tickwire-account/src/events.rs:187-250
ApplyFill == /\ pc = "apply" /\ applied' = applied + 1 /\ pc' = "rest"
             /\ ledger' = IF ledger.booked THEN ledger ELSE
                 [quote |-> ledger.quote - 1, base |-> ledger.base + 1, booked |-> TRUE]
             /\ UNCHANGED <<md,calibrated,window,gap,tx,permit,input,reports>>

\* Inside the gateway's first-accept callback, policy, time and command quota run in order.
\* MaySend uses NODE's observed mark/gap, never the venue's unobserved disconnect.
\* crates/tickwire-runtime-policy/src/lib.rs:900-925 crates/tickwire-runtime-policy/src/lib.rs:1012-1043
\* crates/tickwire-binance-execution/src/lib.rs:525-580 crates/tickwire-account-gateway/src/lib.rs:540-592
MaySend == md.mark /\ gap # "observed" /\ window /\ calibrated /\ permit > 0
FirstWrite == /\ pc = "rest" /\ tx.state = "queued" /\ MaySend
              /\ tx' = [tx EXCEPT !.state = "sent", !.writes = @ + 1,
                        !.writeAuthorized = (permit > 0 /\ window /\ gap # "observed" /\ md.mark)]
              /\ permit' = permit - 1 /\ pc' = "report"
              /\ UNCHANGED <<md,calibrated,window,gap,input,applied,ledger,reports>>
\* A debit can survive a zero-byte attempt. The transport samples the deadline
\* again after finish_gate consumed quota, before accepting the first application byte.
\* crates/tickwire-binance-execution/src/lib.rs:576-582 crates/tickwire-transport/src/connection.rs:188-197
ChargeThenExpire == /\ pc = "rest" /\ tx.state = "queued" /\ MaySend /\ Faults
                    /\ permit' = permit - 1 /\ window' = FALSE
                    /\ tx' = [tx EXCEPT !.state = "aborted"] /\ pc' = "report"
                    /\ UNCHANGED <<md,calibrated,gap,input,applied,ledger,reports>>
\* Rejection with no bytes syncs Aborted, with no private trade or cash movement.
\* crates/tickwire-transport/src/connection.rs:149-209 crates/tickwire-account-gateway/src/lib.rs:1175-1198
DenyFirst == /\ pc = "rest" /\ tx.state = "queued" /\ ~MaySend
             /\ tx' = [tx EXCEPT !.state = "aborted"] /\ pc' = "report"
             /\ UNCHANGED <<md,calibrated,window,gap,permit,input,applied,ledger,reports>>
\* A complete ACK with no buffered surplus is durably captured, but contains no trade in this response profile.
\* crates/tickwire-binance-account/src/rest.rs:208-245 crates/tickwire-binance-account/src/rest/persistence.rs:147-188
\* crates/tickwire-lab/src/fixture.rs:352-395
RestAck == /\ pc = "rest" /\ tx.state = "sent"
           /\ tx' = [tx EXCEPT !.state = "acked", !.ack = TRUE] /\ pc' = "report"
           /\ UNCHANGED <<md,calibrated,window,gap,permit,input,applied,ledger,reports>>
RestIdle == /\ pc = "rest" /\ pc' = "report"
            /\ UNCHANGED <<md,calibrated,window,gap,tx,permit,input,applied,ledger,reports>>
\* The host exports the applied account history, not merely a parsed REST response.
\* crates/tickwire-runtime/src/node.rs:574-599 crates/tickwire-runtime/src/node/strategy.rs:52-70
\* crates/tickwire-strategy-host/src/lib.rs:921-940
ReportPoll == /\ pc = "report" /\ pc' = "strategy" /\ reports' = [reports EXCEPT !.host = ledger.booked]
              /\ UNCHANGED <<md,calibrated,window,gap,tx,permit,input,applied,ledger>>
\* One ordered account-owner call: stage dependencies, register IDs, sign, account admission,
\* prepare the recovery closure, append the local WAL and queue the command.
\* crates/tickwire-runtime/src/node.rs:622-659 crates/tickwire-binance-account/src/runtime.rs:48-106
\* crates/tickwire-runtime/src/authority.rs:146-212 crates/tickwire-binance-execution/src/lib.rs:391-415
\* crates/tickwire-account-gateway/src/lib.rs:421-435 crates/tickwire-account-gateway/src/lib.rs:448-511
Admit == /\ pc = "strategy" /\ md.intent /\ md.mark /\ calibrated /\ window
         /\ gap # "observed" /\ tx.state = "idle"
         /\ tx' = [tx EXCEPT !.reg = TRUE, !.closed = TRUE, !.wal = TRUE, !.state = "queued"]
         /\ pc' = "clock" /\ UNCHANGED <<md,calibrated,window,gap,permit,input,applied,ledger,reports>>
StrategyIdle == /\ pc = "strategy" /\ pc' = "clock"
                /\ UNCHANGED <<md,calibrated,window,gap,tx,permit,input,applied,ledger,reports>>

Next == Publish \/ WorkerDecide \/ WorkerReport \/ Expire \/ LoseSocket \/ ClockPoll \/ MarketPoll
        \/ ObserveGap \/ CaptureFill \/ PrivateIdle \/ ApplyFill \/ FirstWrite \/ DenyFirst
        \/ ChargeThenExpire \/ RestAck \/ RestIdle \/ ReportPoll \/ Admit \/ StrategyIdle
Spec == Init /\ [][Next]_vars
SingleWrite == tx.writes <= 1 /\ permit + tx.writes <= Credit
AdmittedOutput == tx.writes > 0 => tx.reg /\ tx.closed /\ tx.wal /\ tx.writeAuthorized
OneTrade == /\ applied <= input /\ input <= Deliveries
            /\ ledger.base \in 0..1 /\ ledger.quote + ledger.base = 1
            /\ (ledger.base = 1) = (applied > 0)
            /\ (applied > 0 => tx.writes = 1)
ReportPrefix == (reports.child => reports.host) /\ (reports.host => ledger.booked)
AckIsBound == tx.ack => tx.writes = 1 /\ tx.wal
\* Negate terminal events as separate diagnostic probes; the resulting traces show reachability.
NoComplete == ~(reports.child /\ tx.ack /\ ledger.base = 1 /\ applied = Deliveries)
NoFillBeforeAck == ~(reports.child /\ ~tx.ack /\ ledger.base = 1)
=============================================================================
