//! Authenticated read-only trade pages. A page is evidence, never an activation permit.
use crate::*;
use crate::rest::{RestDisposition, RestOwner, RestRecord};
use std::{net::SocketAddr,sync::Arc,time::Duration};
use tickwire_account_gateway::{Authority,Encoder};
use tickwire_account_journal::Backend;
use tickwire_binance::{auth::{Endpoint,MAX_SIGNATURE},params::{Params,Value},request::{Credentials,Market}};
use tickwire_binance_private::{AccountScope,Clock,ClockSample,SignedTime};
use tickwire_transport::{Connection,IoBudget,Limits,PreparedSend,Security,http::{self,ResponseDecoder},rustls};
mod trades;
mod wire;
pub mod bootstrap;
mod snapshot;
pub mod activation;
pub mod orders;
mod closure;
pub use closure::ReconciliationOrder;

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub struct TradeQuery {
    pub intent: IntentKey,
    pub order_id:[u8;16],
    /// Inclusive native trade ID. Zero starts the endpoint's available retained range.
    pub from_id:u64,
    pub limit:u16,
    pub private_connection_epoch:u64,
    pub gap_generation:u64,
}
impl TradeQuery {
    pub(crate) fn validate(self)->Result<()> {
        if self.intent.epoch==0 || self.intent.intent_id==0 || self.order_id==[0;16] || self.from_id>i64::MAX as u64 || self.limit==0 || self.limit>16 || self.private_connection_epoch==0 || self.gap_generation==0 {return Err(Error::Identity);}
        Ok(())
    }
}
#[derive(Clone)]
pub struct ReconcileConfig {
    pub scope:AccountScope,
    pub address:SocketAddr,
    pub tls:Arc<rustls::ClientConfig>,
    pub limits:Limits,
    pub first_connection_epoch:u64,
    pub request_timeout_ns:u64,
    pub recv_window_micros:u64,
    pub max_response_bytes:usize,
    pub max_headers:usize,
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum ReadOperation { Connect, TradePage }
/// The application charges shared connection/request quota here on every retry.
pub trait ReadPermit {
    fn allow(&mut self,scope:&AccountScope,query:&TradeQuery,request_epoch:u64,operation:ReadOperation,now:&ClockSample)->bool;
}
impl<F:FnMut(&AccountScope,&TradeQuery,u64,ReadOperation,&ClockSample)->bool> ReadPermit for F {
    fn allow(&mut self,s:&AccountScope,q:&TradeQuery,e:u64,o:ReadOperation,n:&ClockSample)->bool{self(s,q,e,o,n)}
}
#[derive(Clone,Debug)]
pub struct ReconcilePage {
    pub query:TradeQuery,
    pub connection_epoch:u64,
    pub input:InputKey,
    pub returned:usize,
    pub next_from_id:Option<u64>,
    /// Only the endpoint's retained query range at response time, not account closure.
    pub page_exhausted:bool,
    pub accepted:bool,
    pub ledger_sequence:u64,
    pub through_sequence:u64,
}
struct Pending {
    query:TradeQuery,command:CanonicalCommand,identity:v::OrderIdentity,
    epoch:u64,wire:wire::WireRead,
}
pub struct ReadOnlyReconciler {
    config:ReconcileConfig,next_epoch:u64,pending:Option<Pending>,last:Option<ClockSample>,faulted:bool,
}
impl ReadOnlyReconciler {
    pub fn new(config:ReconcileConfig)->Result<Self>{
        config.scope.validate().map_err(Error::Clock)?;
        if config.first_connection_epoch==0 || config.request_timeout_ns==0 || config.recv_window_micros==0 || config.recv_window_micros>60_000_000 || (config.scope.market==Market::Usdm && !config.recv_window_micros.is_multiple_of(1000)) || config.max_response_bytes==0 || config.max_response_bytes>32*1024 {return Err(Error::Capacity);}
        ResponseDecoder::new(config.max_response_bytes,config.max_headers).map_err(Error::Transport)?;
        Ok(Self{next_epoch:config.first_connection_epoch,config,pending:None,last:None,faulted:false})
    }
    pub fn cancel(&mut self) { self.pending=None; }
    pub fn request_epoch(&self)->Option<u64>{self.pending.as_ref().map(|p|p.epoch)}
    pub fn busy(&self)->bool{self.pending.is_some()}
    pub fn start<S:Store,B:Backend,A:Authority,E:Encoder>(&mut self,owner:&mut RestOwner<S,B,A,E>,query:TradeQuery,credentials:&Credentials<'_>,clock:&mut impl Clock,permit:&mut impl ReadPermit)->Result<()> {
        owner.gateway.freeze_account();
        if self.faulted {return Err(Error::Stopped);}
        if self.pending.is_some(){return Err(Error::Busy);}
        owner.ingress.ready(&owner.gateway)?;query.validate()?;
        let scope=&owner.ingress.config.baseline.snapshot().scope;
        if self.config.scope.risk_domain_id!=scope.risk_domain_id || self.config.scope.venue!=scope.venue || self.config.scope.account!=scope.account || self.config.scope.environment!=scope.environment || self.config.scope.market!=owner.ingress.market()? || credentials.market!=self.config.scope.market || credentials.account!=scope.account || credentials.environment!=scope.environment {return Err(Error::Identity);}
        if self.config.max_response_bytes>owner.ingress.config.max_raw_bytes {return Err(Error::Capacity);}
        let reg=owner.ingress.registry.get(&query.intent).ok_or(Error::Identity)?;
        let identity=reg.orders.iter().find(|id|id.order_id==query.order_id).ok_or(Error::Identity)?.clone();
        if owner.gateway.command(query.intent)!=Some(&reg.command) || identity.venue_order_id.is_none() {return Err(Error::Identity);}
        let command=reg.command.clone();
        let started=self.sample(clock)?;
        let stamp=started.signing_time.ok_or(Error::Clock(tickwire_binance_private::Error::Clock))?;
        source(&self.config.scope,&stamp,&started)?;
        clock.check_signed(&self.config.scope,&stamp,&started,self.config.recv_window_micros).map_err(Error::Clock)?;
        let bytes=encode(&self.config,&command,&identity,query,credentials,stamp)?;
        let epoch=self.next_epoch;self.next_epoch=self.next_epoch.checked_add(1).ok_or(Error::Capacity)?;
        if !permit.allow(&self.config.scope,&query,epoch,ReadOperation::Connect,&started){return Err(Error::Stopped);}
        let now=self.sample(clock)?;
        let deadline_mono=started.monotonic_ns().checked_add(self.config.request_timeout_ns).ok_or(Error::Capacity)?;
        let deadline=started.instant.checked_add(Duration::from_nanos(self.config.request_timeout_ns)).ok_or(Error::Capacity)?;
        if now.monotonic_ns()>=deadline_mono || now.instant>=deadline {return Err(Error::Stopped);}
        let wire=wire::WireRead::new(&self.config,stamp,started,&bytes)?;
        self.pending=Some(Pending{query,command,identity,epoch,wire});
        Ok(())
    }
    fn sample(&mut self,clock:&mut impl Clock)->Result<ClockSample>{
        let now=clock.sample().map_err(Error::Clock)?;
        if self.last.is_some_and(|old|now.monotonic.domain()!=old.monotonic.domain() || now.monotonic_ns()<old.monotonic_ns() || now.instant<old.instant){self.faulted=true;return Err(Error::Clock(tickwire_binance_private::Error::ClockDomain));}
        self.last=Some(now);Ok(now)
    }
    pub fn poll<S:Store,B:Backend,A:Authority,E:Encoder>(&mut self,owner:&mut RestOwner<S,B,A,E>,clock:&mut impl Clock,permit:&mut impl ReadPermit,budget:IoBudget)->Result<Option<ReconcilePage>>{
        let Some(p)=self.pending.as_mut() else{return Ok(None);};
        let scope=&self.config.scope;let query=p.query;
        let result=p.wire.poll(&self.config,clock,&mut self.last,budget,|now|permit.allow(scope,&query,p.epoch,ReadOperation::TradePage,now));
        let response=match result {Ok(Some(response))=>response,Ok(None)=>return Ok(None),Err(error)=>{if matches!(error,Error::Clock(tickwire_binance_private::Error::ClockDomain)){self.faulted=true;}return Err(error);}};
        let p=self.pending.take().unwrap();
        let body=response.body.as_deref().filter(|_|response.status==200);
        owner.gateway.freeze_account();owner.ingress.ready(&owner.gateway)?;
        let mut record=RestRecord{command:p.command,query:Some(p.query),order_query:None,epoch:p.epoch,received:response.received,status:response.status,raw:response.raw.clone(),disposition:RestDisposition::Unknown,events:Vec::new()};
        if let Some(body)=body {
            if let Ok(events)=trades::bind(&record.command,&p.identity,&owner.ingress.config.baseline.snapshot().assets,p.query,p.epoch,record.received,body){
                let mut candidate=owner.gateway.account().clone();
                if events.iter().all(|event|candidate.apply_event(event).is_ok()){record.disposition=RestDisposition::NeedsReconciliation;record.events=events;}
            }
        }
        let accepted=record.disposition==RestDisposition::NeedsReconciliation;
        let returned=record.events.len();
        let next_from_id=record.events.last().and_then(|e|match &e.event().body{v::VenueEventBody::Fill(fill)=>fill.trade_id.as_str().parse::<u64>().ok()?.checked_add(1),_=>None});
        let outcome=owner.ingress.commit_rest(&mut owner.gateway,record)?;
        Ok(Some(ReconcilePage{query:p.query,connection_epoch:p.epoch,input:outcome.input.unwrap(),returned,next_from_id,page_exhausted:accepted&&returned<p.query.limit as usize,accepted,ledger_sequence:owner.account().ledger_sequence(),through_sequence:owner.applied_position()}))
    }
    pub fn register(&mut self,registry:&tickwire_transport::mio::Registry,token:tickwire_transport::mio::Token)->Result<()>{self.pending.as_mut().ok_or(Error::Busy)?.wire.connection.register(registry,token).map_err(Error::Transport)}
    pub fn reregister(&mut self,registry:&tickwire_transport::mio::Registry,token:tickwire_transport::mio::Token)->Result<()>{self.pending.as_mut().ok_or(Error::Busy)?.wire.connection.reregister(registry,token).map_err(Error::Transport)}
}
fn authority(scope:&AccountScope)->&'static str{match(scope.market,scope.environment){(Market::Spot,v::Environment::Production)=>"api.binance.com",(Market::Spot,v::Environment::Test)=>"testnet.binance.vision",(Market::Usdm,v::Environment::Production)=>"fapi.binance.com",(Market::Usdm,v::Environment::Test)=>"demo-fapi.binance.com"}}
fn source(scope:&AccountScope,stamp:&SignedTime,now:&ClockSample)->Result<()> {
    use tickwire_clock::Source;
    let expected=match(scope.market,scope.environment){(Market::Spot,v::Environment::Production)=>Source::SpotProduction,(Market::Spot,v::Environment::Test)=>Source::SpotTest,(Market::Usdm,v::Environment::Production)=>Source::UsdmProduction,(Market::Usdm,v::Environment::Test)=>Source::UsdmTest};
    if stamp.source()!=expected || stamp.domain()!=now.monotonic.domain(){return Err(Error::Identity);}Ok(())
}
fn fresh(stamp:SignedTime,now:&ClockSample,window:u64)->bool {
    fn micros(stamp:SignedTime)->u128 {u128::from(stamp.timestamp())*if stamp.unit()==tickwire_clock::TimeUnit::Milliseconds{1000}else{1}}
    now.signing_time.is_some_and(|current|current.domain()==stamp.domain()&&current.source()==stamp.source()&&micros(stamp)<micros(current)+1_000_000&&micros(current)<=micros(stamp)+u128::from(window))
}
fn encode(config:&ReconcileConfig,command:&CanonicalCommand,identity:&v::OrderIdentity,query:TradeQuery,credentials:&Credentials<'_>,stamp:SignedTime)->Result<Vec<u8>>{
    if credentials.api_key.is_empty()||credentials.api_key.len()>256||!credentials.api_key.bytes().all(|b|b.is_ascii_alphanumeric()||b"-_".contains(&b)){return Err(Error::Identity);}
    let order=identity.venue_order_id.as_ref().ok_or(Error::Identity)?.as_str().parse::<u64>().map_err(|_|Error::Identity)?;
    if order>i64::MAX as u64{return Err(Error::Identity);}
    let window=if config.recv_window_micros.is_multiple_of(1000){(config.recv_window_micros/1000).to_string()}else{format!("{}.{:03}",config.recv_window_micros/1000,config.recv_window_micros%1000).trim_end_matches('0').to_owned()};
    let mut params=Params::<8>::new();
    params.push("symbol",Value::String(command.command().context.instrument.symbol.as_str())).map_err(|_|Error::Encoding)?;
    for (key,value) in [("orderId",order),("fromId",query.from_id),("limit",query.limit as u64),("timestamp",stamp.timestamp())]{params.push(key,Value::Integer(value)).map_err(|_|Error::Encoding)?;}
    params.push("recvWindow",Value::Number(&window)).map_err(|_|Error::Encoding)?;
    if config.scope.market==Market::Usdm && stamp.unit()!=tickwire_clock::TimeUnit::Milliseconds{return Err(Error::Identity);}
    let mut query_bytes=[0u8;8192];let n=params.rest(&mut query_bytes).map_err(|_|Error::Encoding)?;
    let endpoint=if config.scope.market==Market::Spot{Endpoint::SpotRest}else{Endpoint::UsdmRest};
    let mut signature=[0u8;MAX_SIGNATURE];let size=credentials.signing_key.sign(endpoint,&query_bytes[..n],&mut signature).map_err(|_|Error::Encoding)?;
    let mut signed=String::from_utf8(query_bytes[..n].to_vec()).map_err(|_|Error::Encoding)?;signed.push_str("&signature=");
    for b in &signature[..size]{if b.is_ascii_alphanumeric()||b"-._~".contains(b){signed.push(*b as char);}else{signed.push_str(&format!("%{b:02X}"));}}
    let endpoint=if config.scope.market==Market::Spot{"/api/v3/myTrades"}else{"/fapi/v1/userTrades"};
    http::encode_request("GET",&format!("{endpoint}?{signed}"),&[("Host",authority(&config.scope)),("X-MBX-APIKEY",credentials.api_key)],&[],config.limits.max_send_bytes).map_err(Error::Transport)
}
