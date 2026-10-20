use super::*;
use orders::OrderQuery;
use tickwire_account::{OrderKey,OrderView,Fixed,RecoveryRecord};
#[derive(Clone,Debug)]
pub struct ReconciliationOrder {
    pub intent:IntentKey,
    pub order_id:[u8;16],
    pub identity:v::OrderIdentity,
    pub state:OrderView,
    pub latest_status:Option<v::OrderStatus>,
    pub cumulative_quantity:Option<v::Decimal>,
}
impl<S:Store,B:Backend,A:Authority,E:Encoder> RestOwner<S,B,A,E>{
    pub fn reconciliation_orders(&self)->Vec<ReconciliationOrder>{
        let mut orders=Vec::new();
        for (intent,reg) in &self.ingress.registry {
            if self.gateway.command(*intent)!=Some(&reg.command){continue;}
            for identity in &reg.orders {
                let Some(client_id)=identity.client_id.clone() else{continue;};
                let Some(state)=self.account().order(&OrderKey{intent:*intent,client_id}) else{continue;};
                if state.aborted || (state.terminal&&state.fee_reconciled&&!state.unknown){continue;}
                let mut latest_status=None;let mut cumulative_quantity=None;
                for index in (0..self.ingress.records.len()).rev(){
                    let Some(event)=self.ingress.effect(index) else{continue;};
                    let observation=match &event.event().body {v::VenueEventBody::Order(o)=>o.as_ref(),v::VenueEventBody::Fill(f)=>&f.observation,_=>continue};
                    if observation.order.order_id==identity.order_id {
                        if latest_status.is_none()&&observation.status!=v::OrderStatus::Unknown{latest_status=Some(observation.status);}
                        if cumulative_quantity.is_none(){cumulative_quantity=observation.cumulative_quantity;}
                        if latest_status.is_some()&&cumulative_quantity.is_some(){break;}
                    }
                }
                orders.push(ReconciliationOrder{intent:*intent,order_id:identity.order_id,identity:identity.clone(),state,latest_status,cumulative_quantity});
            }
        }
        orders
    }
    /// Require a complete native trade page chain from zero whose actual fills
    /// cover the status query's cumulative quantity. Terminal fee closure is then durable.
    pub fn complete_trade_reconciliation(&mut self,query:OrderQuery)->Result<()> {
        self.gateway.freeze_account();self.ingress.ready(&self.gateway)?;query.validate()?;
        let reg=self.ingress.registry.get(&query.intent).ok_or(Error::Identity)?;
        let identity=reg.orders.iter().find(|o|o.order_id==query.order_id).ok_or(Error::Identity)?;
        let key=OrderKey{intent:query.intent,client_id:identity.client_id.clone().ok_or(Error::Identity)?};
        let view=self.account().order(&key).ok_or(Error::Identity)?;
        let start=self.ingress.records.iter().rposition(|r|matches!(r,Record::Rest(r) if r.order_query==Some(query)&&r.events.len()==1&&matches!(r.disposition,RestDisposition::Observed|RestDisposition::NeedsReconciliation))).ok_or(Error::Identity)?;
        let Record::Rest(status)=&self.ingress.records[start] else{unreachable!()};
        let v::VenueEventBody::Order(observation)=&status.events[0].event().body else{return Err(Error::Identity);};
        if observation.status==v::OrderStatus::Unknown || view.terminal!=matches!(observation.status,v::OrderStatus::Filled|v::OrderStatus::Canceled|v::OrderStatus::Expired|v::OrderStatus::Rejected){return Err(Error::Conflict);}
        let expected=observation.cumulative_quantity.ok_or(Error::Conflict)?;
        let expected=Fixed::parse(&expected.as_plain_string(),view.filled_quantity.scale()).map_err(|_|Error::Encoding)?;
        let mut total=Fixed::from_atoms(0,view.filled_quantity.scale()).map_err(|_|Error::Encoding)?;
        let mut next=0;let mut complete=false;let mut seen=false;
        for record in &self.ingress.records[start+1..] {
            match record {
                Record::Gap{..}|Record::BeginReconciliation{..}=>return Err(Error::Identity),
                Record::Rest(record) if record.query.is_some_and(|q|q.intent==query.intent&&q.order_id==query.order_id)=>{
                    let q=record.query.unwrap();
                    if complete||q.private_connection_epoch!=query.private_connection_epoch||q.gap_generation!=query.gap_generation
                        ||q.from_id!=next||record.disposition!=RestDisposition::NeedsReconciliation {return Err(Error::Conflict);}
                    seen=true;
                    for event in &record.events {
                        let v::VenueEventBody::Fill(fill)=&event.event().body else{return Err(Error::Identity);};
                        if fill.observation.order.order_id!=query.order_id||fill.fees.as_slice().is_empty(){return Err(Error::Identity);}
                        let quantity=Fixed::parse(&fill.last_quantity.as_plain_string(),total.scale()).map_err(|_|Error::Encoding)?;
                        total=total.checked_add(quantity).map_err(|_|Error::Capacity)?;
                        next=fill.trade_id.as_str().parse::<u64>().map_err(|_|Error::Encoding)?.checked_add(1).ok_or(Error::Capacity)?;
                    }
                    complete=record.events.len()<q.limit as usize;
                }
                _=>{}
            }
        }
        if !seen||!complete||total!=expected||total!=view.filled_quantity{return Err(Error::Conflict);}
        if view.terminal&&!view.fee_reconciled {
            let mut candidate=self.account().clone();candidate.reconcile_fees(&key,total).map_err(|_|Error::Conflict)?;
            self.gateway.record_fact(RecoveryRecord::FeesReconciled{order:key,cumulative:total}).map_err(Error::Gateway)?;
        }
        Ok(())
    }
}
