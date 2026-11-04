----------------------------- MODULE L0_TerminalFees -----------------------------
EXTENDS Integers, FiniteSets, Sequences
CONSTANT MaxFills, AllowRegression
(*
One admitted BTC buy. Each native trade ID buys one base unit for two quote units
and charges one quote unit. The selected order has sufficient cash in its baseline.
Variables and actions below refer to production source at 9b2ed63.
phase = sole account owner's position; scan worker can deliver only a stable cut:
  crates/tickwire-spot-runner/src/live.rs:265-273, crates/tickwire-spot-runner/src/live/account.rs:141-158.
wsQty = cumulative size in a captured private terminal row, or -1 if absent:
  crates/tickwire-spot-runner/src/reconcile/ingest.rs:209-223.
scan = first authenticated economic summary; second fingerprint and time checked
  at crates/tickwire-spot-runner/src/reconcile/proof.rs:80-116.
nextId = position in the second scan's per-trade list; facts = booked trade IDs:
  crates/tickwire-spot-runner/src/reconcile/ingest.rs:245-269,
  crates/tickwire-account/src/events.rs:54-102, crates/tickwire-account/src/events.rs:107-125.
debit = spent quote plus actual per-trade commission:
  crates/tickwire-account/src/events.rs:244-251, crates/tickwire-account/src/events.rs:285-305, crates/tickwire-account/src/events.rs:307-346.
terminal = maximum observed terminal cumulative size, not booked quantity:
  crates/tickwire-account/src/events.rs:411-446.
wal = ordered projection of synchronized IngressEvent / FeesReconciled records:
  crates/tickwire-account-gateway/src/lib.rs:648-679, crates/tickwire-account-gateway/src/lib.rs:1026-1050.
A fresh cursor for a repeated trade still produces an ingress record. A retry on
the identical cursor is a stutter in the projection. A crash discards owner state.
*)
VARIABLES phase, wsQty, scan, nextId, facts, debit, terminal, wal
vars == <<phase, wsQty, scan, nextId, facts, debit, terminal, wal>>
Ids == 1..MaxFills
Summaries == [qty : 0..MaxFills, trades : 0..MaxFills, walletAgrees : BOOLEAN]
EmptyScan == [qty |-> 0, trades |-> 0, walletAgrees |-> TRUE]
Ord(a,b) == IF a>b THEN a ELSE b
Row(k,v) == <<k,v>>
Fill(k) == k \in {"ws","rest"}
TradeIds(s) == {s[i][2] : i \in {j \in 1..Len(s) : Fill(s[j][1])}}
WSIds(s) == {s[i][2] : i \in {j \in 1..Len(s) : s[j][1]="ws"}}

Init == /\ phase="ready" /\ wsQty=-1 /\ scan=EmptyScan /\ nextId=1
        /\ facts={} /\ debit=0 /\ terminal=-1 /\ wal = <<>>

\* A new source cursor enters AccountGateway::record_ingress_event.
\* crates/tickwire-spot-runner/src/reconcile/ingest.rs:189-207, crates/tickwire-spot-runner/src/reconcile/ingest.rs:245-269;
\* crates/tickwire-account-gateway/src/lib.rs:648-679; crates/tickwire-account/src/events.rs:82-125, crates/tickwire-account/src/events.rs:244-251, crates/tickwire-account/src/events.rs:291-294.
Book(k,i) == /\ facts'=facts \cup {i} /\ debit'=debit + (IF i \in facts THEN 0 ELSE 3)
             /\ wal'=Append(wal,Row(k,i))
WSFill == /\ phase="ready" /\ \E i \in Ids \ WSIds(wal): Book("ws",i)
          /\ UNCHANGED <<phase,wsQty,scan,nextId,terminal>>

\* Private order channel -> record_order -> Account::observe.
\* crates/tickwire-spot-runner/src/reconcile/ingest.rs:145-161, crates/tickwire-spot-runner/src/reconcile/ingest.rs:209-223;
\* crates/tickwire-account/src/events.rs:411-446.
WSTerminal == /\ phase="ready" /\ wsQty=-1
              /\ \E q \in 0..MaxFills: /\ wsQty'=q /\ terminal'=q
                                      /\ wal'=Append(wal,Row("terminal",q))
              /\ UNCHANGED <<phase,scan,nextId,facts,debit>>

\* Only completed reads with identical local account position and private cursor enter the owner.
\* crates/tickwire-spot-runner/src/live/scan.rs:56-71; crates/tickwire-spot-runner/src/live.rs:265-273.
ReadFirst == /\ phase="ready"
             /\ \E r \in Summaries: /\ (AllowRegression \/ r.qty>=wsQty) /\ scan'=r
             /\ phase'="first" /\ UNCHANGED <<wsQty,nextId,facts,debit,terminal,wal>>

\* Two different economic fingerprints fail before applying fills.
\* crates/tickwire-spot-runner/src/reconcile/proof.rs:87-116, crates/tickwire-spot-runner/src/reconcile.rs:152-157.
ReadSecond == /\ phase="first" /\ \E r \in Summaries:
                  /\ (AllowRegression \/ r.qty>=wsQty)
                  /\ phase'=(IF r=scan THEN "fills" ELSE "denied")
              /\ UNCHANGED <<wsQty,scan,nextId,facts,debit,terminal,wal>>

\* The scan includes one ordered prefix of known native IDs.
\* crates/tickwire-spot-runner/src/reconcile/ingest.rs:245-269.
RESTFill == /\ phase="fills" /\ nextId<=scan.trades
            /\ Book("rest",nextId) /\ nextId'=nextId+1
            /\ UNCHANGED <<phase,wsQty,scan,terminal>>

\* Cumulative must equal locally booked quantity before recording the REST terminal.
\* crates/tickwire-spot-runner/src/reconcile/ingest.rs:270-294; crates/tickwire-account/src/events.rs:423-442.
RESTTerminal == /\ phase="fills" /\ nextId>scan.trades
                /\ IF scan.qty=Cardinality(facts)
                      THEN /\ phase'="wallet" /\ terminal'=Ord(terminal,scan.qty)
                           /\ wal'=Append(wal,Row("terminal",scan.qty))
                      ELSE /\ phase'="denied" /\ UNCHANGED <<terminal,wal>>
                /\ UNCHANGED <<wsQty,scan,nextId,facts,debit>>

\* The remote wallet reflects its fills. Exact total/locks and remaining-order checks apply.
\* crates/tickwire-spot-runner/src/reconcile/compare.rs:125-148, crates/tickwire-spot-runner/src/reconcile/compare.rs:180-221, crates/tickwire-spot-runner/src/reconcile/compare.rs:245-263;
\* crates/tickwire-spot-runner/src/reconcile/ingest.rs:295-299.
Wallet == /\ phase="wallet"
          /\ phase'=(IF scan.walletAgrees /\ debit=3*scan.trades /\ facts=1..scan.trades
                     THEN "close" ELSE "denied")
          /\ UNCHANGED <<wsQty,scan,nextId,facts,debit,terminal,wal>>

\* Ingress::apply_scan passes local.filled_quantity. Gateway synchronizes before apply_fact.
\* crates/tickwire-spot-runner/src/reconcile/ingest.rs:298-315;
\* crates/tickwire-account-gateway/src/lib.rs:1042-1049; crates/tickwire-account-journal/src/lib.rs:514-529.
AppendFee == /\ phase="close" /\ phase'="durable"
             /\ wal'=Append(wal,Row("fee",Cardinality(facts)))
             /\ UNCHANGED <<wsQty,scan,nextId,facts,debit,terminal>>

\* Real precondition also compares the stored terminal cumulative with the supplied value.
\* crates/tickwire-account/src/admission.rs:484-496; crates/tickwire-account-gateway/src/lib.rs:1046-1050.
ApplyFee == /\ phase="durable"
            /\ phase'=(IF terminal=Cardinality(facts) THEN "checked" ELSE "denied")
            /\ UNCHANGED <<wsQty,scan,nextId,facts,debit,terminal,wal>>

\* Private terminal row must be covered by booked fills when verifying the archived cut.
\* crates/tickwire-spot-runner/src/reconcile/compare.rs:319-332; crates/tickwire-spot-runner/src/reconcile/proof.rs:154-180;
\* crates/tickwire-spot-runner/src/reconcile.rs:181-192; crates/tickwire-spot-runner/src/live/account.rs:151-159. Other grant guards fixed satisfied.
Install == /\ phase="checked"
           /\ phase'=(IF wsQty<=Cardinality(facts) THEN "qualified" ELSE "denied")
           /\ UNCHANGED <<wsQty,scan,nextId,facts,debit,terminal,wal>>

\* Crash can occur before fee append, after sync before application, or after rejected apply.
\* crates/tickwire-account-journal/src/lib.rs:246-266, crates/tickwire-account-journal/src/lib.rs:417-426; crates/tickwire-account/src/recovery.rs:6-18.
Crash == /\ phase \notin {"reopen","replayed","unreadable"}
         /\ phase'="reopen" /\ facts'={} /\ debit'=0 /\ terminal'=-1
         /\ UNCHANGED <<wsQty,scan,nextId,wal>>

\* Rebuild order and trade indexes from each earlier WAL prefix and replay every fee record.
\* crates/tickwire-account/src/recovery.rs:48-50, crates/tickwire-account/src/recovery.rs:64-65, crates/tickwire-account/src/recovery.rs:110-121; crates/tickwire-account/src/admission.rs:484-496.
Prior(s,i) == SubSeq(s,1,i-1)
Terminals(s) == {s[i][2] : i \in {j \in 1..Len(s) : s[j][1]="terminal"}}
TerminalMax(s) == CHOOSE m \in Terminals(s) \cup {-1}:
                            \A t \in Terminals(s): m>=t
Replayable(s) == \A i \in 1..Len(s): s[i][1]="fee" =>
                 /\ s[i][2]=Cardinality(TradeIds(Prior(s,i)))
                 /\ s[i][2]=TerminalMax(Prior(s,i))
Replay == /\ phase="reopen" /\ phase'=(IF Replayable(wal) THEN "replayed" ELSE "unreadable")
          /\ facts'=TradeIds(wal) /\ debit'=3*Cardinality(TradeIds(wal))
          /\ terminal'=TerminalMax(wal) /\ UNCHANGED <<wsQty,scan,nextId,wal>>

Next == WSFill \/ WSTerminal \/ ReadFirst \/ ReadSecond \/ RESTFill \/ RESTTerminal
        \/ Wallet \/ AppendFee \/ ApplyFee \/ Install \/ Crash \/ Replay
Spec == Init /\ [][Next]_vars
CashOnce == phase \notin {"reopen","unreadable"} => facts=TradeIds(wal) /\ debit=3*Cardinality(facts)
QualifiedCash == phase="qualified" => /\ Replayable(wal) /\ scan.qty=Cardinality(facts)
                                      /\ debit=3*scan.trades /\ wsQty<=scan.qty
RestartReadable == phase # "unreadable"
=============================================================================
