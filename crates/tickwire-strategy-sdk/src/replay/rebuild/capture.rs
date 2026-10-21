//! Bounded input capture. No user factory is reachable from this owner.
use super::*;
use tickwire_clock::MonoTime;
use tickwire_release::rebuild::window::{Identity,Seal};
pub struct CaptureOnly;
pub struct Captured { seal:Seal, inputs:Inputs }
impl Captured {
    pub fn seal(&self)->&Seal {&self.seal}
    pub fn inputs(&self)->&Inputs {&self.inputs}
}
impl CaptureOnly {
    pub fn new(_identity:Identity,_contract:Contract,_seed:Inputs,_now:MonoTime)->Result<Self>{Err(Error::Unsupported)}
    pub fn push(&mut self,_input:Input,_now:MonoTime)->Result<()> {Err(Error::Unsupported)}
    pub fn finish(self)->Result<Captured>{Err(Error::Unsupported)}
}
