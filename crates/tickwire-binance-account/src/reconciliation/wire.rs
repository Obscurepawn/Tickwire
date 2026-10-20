use super::*;
pub(super) struct WireRead {
    pub connection:Connection,
    decoder:ResponseDecoder,
    raw:Vec<u8>,ended:bool,invalid:bool,
    stamp:SignedTime,deadline_mono:u64,deadline:std::time::Instant,
}
pub(super) struct WireResponse {pub raw:Vec<u8>,pub status:u16,pub body:Option<Vec<u8>>,pub received:u64}
impl WireRead {
    pub fn new(config:&ReconcileConfig,stamp:SignedTime,started:ClockSample,bytes:&[u8])->Result<Self>{
        let deadline_mono=started.monotonic_ns().checked_add(config.request_timeout_ns).ok_or(Error::Capacity)?;
        let deadline=started.instant.checked_add(Duration::from_nanos(config.request_timeout_ns)).ok_or(Error::Capacity)?;
        let mut connection=Connection::connect(config.address,Security::Tls{server_name:authority(&config.scope).to_owned().try_into().map_err(|_|Error::Identity)?,config:config.tls.clone()},config.limits,deadline).map_err(Error::Transport)?;
        connection.queue(PreparedSend::new(bytes,deadline,config.limits.max_send_bytes).map_err(Error::Transport)?).map_err(Error::Transport)?;
        Ok(Self{connection,decoder:ResponseDecoder::new(config.max_response_bytes,config.max_headers).map_err(Error::Transport)?,raw:Vec::new(),ended:false,invalid:false,stamp,deadline_mono,deadline})
    }
    pub fn poll(&mut self,config:&ReconcileConfig,clock:&mut impl Clock,last:&mut Option<ClockSample>,budget:IoBudget,mut permit:impl FnMut(&ClockSample)->bool)->Result<Option<WireResponse>>{
        if budget.max_bytes==0||budget.max_syscalls==0{return Err(Error::Capacity);}
        let now=clock.sample().map_err(Error::Clock)?;ordered(last,now)?;source(&config.scope,&self.stamp,&now)?;
        if !self.ended {
            match self.connection.drive(now.instant,budget){Ok(report)=>self.ended=report.peer_closed,Err(_)=>self.ended=true}
            if !self.ended {
                let stamp=self.stamp;let deadline_mono=self.deadline_mono;let deadline=self.deadline;
                let mut clock_error=None;
                let sent=self.connection.try_send(now.instant,||{
                    let result=(||->Result<bool>{
                        let first=clock.sample().map_err(Error::Clock)?;ordered(last,first)?;source(&config.scope,&stamp,&first)?;
                        clock.check_signed(&config.scope,&stamp,&first,config.recv_window_micros).map_err(Error::Clock)?;
                        if !permit(&first){return Ok(false);}
                        let second=clock.sample().map_err(Error::Clock)?;ordered(last,second)?;source(&config.scope,&stamp,&second)?;
                        clock.check_signed(&config.scope,&stamp,&second,config.recv_window_micros).map_err(Error::Clock)?;
                        let final_sample=clock.sample().map_err(Error::Clock)?;ordered(last,final_sample)?;source(&config.scope,&stamp,&final_sample)?;
                        Ok(final_sample.monotonic_ns()<deadline_mono&&final_sample.instant<deadline&&fresh(stamp,&final_sample,config.recv_window_micros))
                    })();
                    match result {Ok(allowed)=>allowed,Err(error)=>{clock_error=Some(error);false}}
                });
                if sent.is_err(){self.ended=true;}
                if let Some(error)=clock_error{self.ended=true;self.invalid=true;return Err(error);}
            }
        }
        let mut bytes=[0u8;4096];let mut drained=0;
        while drained<budget.max_bytes {
            let limit=bytes.len().min(budget.max_bytes-drained);let n=self.connection.read(&mut bytes[..limit]);if n==0{break;}drained+=n;
            if self.raw.len().checked_add(n).is_none_or(|n|n>config.max_response_bytes){self.ended=true;self.invalid=true;break;}
            self.raw.extend_from_slice(&bytes[..n]);if self.decoder.push(&bytes[..n]).is_err(){self.ended=true;self.invalid=true;break;}
        }
        if drained==budget.max_bytes{return Ok(None);}
        if self.ended{self.decoder.finish();}
        let parsed=self.decoder.next_response();
        let report=self.connection.send_report();
        let complete=report.is_some_and(|r|r.accepted_bytes==r.total_bytes&&r.total_bytes>0);
        let terminal=report.is_some_and(|r|matches!(r.state,tickwire_transport::SendState::NoBytes(_)|tickwire_transport::SendState::Failed(_)));
        if !(parsed.as_ref().is_ok_and(Option::is_some)||parsed.is_err()||self.ended||terminal||now.monotonic_ns()>=self.deadline_mono||now.instant>=self.deadline){return Ok(None);}
        let response=parsed.ok().flatten();
        let status=response.as_ref().map_or(0,|r|r.status);
        let body=response.filter(|_|complete&&!self.invalid&&self.decoder.take_buffered().is_empty()).map(|r|r.body);
        Ok(Some(WireResponse{raw:std::mem::take(&mut self.raw),status,body,received:last.unwrap_or(now).monotonic_ns()}))
    }
}
pub(super) fn ordered(last:&mut Option<ClockSample>,now:ClockSample)->Result<()> {
    if last.is_some_and(|old|now.monotonic.domain()!=old.monotonic.domain()||now.monotonic_ns()<old.monotonic_ns()||now.instant<old.instant){return Err(Error::Clock(tickwire_binance_private::Error::ClockDomain));}
    *last=Some(now);Ok(())
}
