------------------------------ MODULE L0_PrivateCut ------------------------------
EXTENDS Naturals, Sequences

CONSTANT MaxRows
VARIABLES online, epoch, log, applied, tradeBooked, bookings, scan, proof, grant, sent, bad
vars == <<online, epoch, log, applied, tradeBooked, bookings, scan, proof, grant, sent, bad>>
None == <<0,0>>
Cut == <<epoch,Len(log)>>
Current == online /\ applied = Len(log) /\ grant = Cut
Init == /\ online = FALSE /\ epoch = 0 /\ log = <<>> /\ applied = 0
        /\ tradeBooked = FALSE /\ bookings = 0 /\ scan = None /\ proof = None
        /\ grant = None /\ sent = FALSE /\ bad = FALSE

Connect == /\ ~online /\ epoch < 2 /\ online' = TRUE /\ epoch' = epoch + 1
           /\ grant' = None /\ UNCHANGED <<log, applied, tradeBooked, bookings, scan, proof, sent, bad>>
Disconnect == /\ online /\ online' = FALSE /\ grant' = None
              /\ UNCHANGED <<epoch, log, applied, tradeBooked, bookings, scan, proof, sent, bad>>
ReceiveFill == /\ online /\ Len(log) < MaxRows /\ log' = Append(log,"trade")
               /\ UNCHANGED <<online,epoch,applied,tradeBooked,bookings,scan,proof,grant,sent,bad>>
Consume == /\ applied < Len(log) /\ applied' = applied + 1
           /\ tradeBooked' = TRUE /\ bookings' = IF tradeBooked THEN bookings ELSE bookings + 1
           /\ grant' = IF online /\ grant = <<epoch,applied>> THEN <<epoch,applied'>> ELSE None
           /\ UNCHANGED <<online,epoch,log,scan,proof,sent,bad>>
BeginScan == /\ online /\ applied = Len(log) /\ scan = None /\ scan' = Cut
             /\ UNCHANGED <<online,epoch,log,applied,tradeBooked,bookings,proof,grant,sent,bad>>
Discard == /\ scan # None /\ scan' = None
           /\ UNCHANGED <<online,epoch,log,applied,tradeBooked,bookings,proof,grant,sent,bad>>
Commit == /\ online /\ applied = Len(log) /\ scan = Cut
          /\ proof' = scan /\ scan' = None
          /\ UNCHANGED <<online,epoch,log,applied,tradeBooked,bookings,grant,sent,bad>>
Install == /\ online /\ applied = Len(log) /\ proof = Cut /\ grant # Cut /\ grant' = Cut
           /\ UNCHANGED <<online,epoch,log,applied,tradeBooked,bookings,scan,proof,sent,bad>>
FirstWrite == /\ ~sent /\ Current /\ sent' = TRUE
              /\ bad' = (bad \/ proof = None \/ proof[1] # epoch \/ grant[2] # Len(log))
              /\ UNCHANGED <<online,epoch,log,applied,tradeBooked,bookings,scan,proof,grant>>
End == /\ UNCHANGED vars
Next == Connect \/ Disconnect \/ ReceiveFill \/ Consume \/ BeginScan \/ Discard \/ Commit \/ Install \/ FirstWrite \/ End
Safe == /\ ~bad /\ bookings <= 1 /\ applied <= Len(log)
        /\ (Current => (proof # None /\ proof[1] = epoch))
=============================================================================
