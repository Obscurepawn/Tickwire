use std::{collections::BTreeSet,fs,path::Path,os::unix::fs::{DirBuilderExt,PermissionsExt}};
use tickwire_app::observation::{Outcome,Status};
use tickwire_release::Object;
fn hex(v:&[u8])->String {v.iter().map(|v|format!("{v:02x}")).collect()}
fn copy_store(from:&Path,to:&Path)->Result<(),Box<dyn std::error::Error>> {
 fs::DirBuilder::new().mode(0o700).create(to)?;
 let mut total=0u64;
 for entry in fs::read_dir(from)? {
  let entry=entry?; if entry.file_name()=="LOCK" {continue;}
  let m=entry.metadata()?;assert!(entry.file_type()?.is_file() && m.len()<=64*1024*1024);
  total+=m.len();assert!(total<=256*1024*1024);fs::copy(entry.path(),to.join(entry.file_name()))?;
 }
 Ok(())
}
fn main()->Result<(),Box<dyn std::error::Error>> {
 let args:Vec<_>=std::env::args().collect();
 if args.get(1).is_some_and(|s|s=="--mirror-prefix") {
  assert_eq!(args.len(),4);let mut config=tickwire_replica::config::ReceiverConfig::from_file(&args[2])?;let scratch=Path::new(&args[3]);
  fs::DirBuilder::new().mode(0o700).create(scratch)?;
  copy_store(&config.journal_dir,&scratch.join("journal"))?;copy_store(&config.object_dir,&scratch.join("objects"))?;
  config.journal_dir=scratch.join("journal");config.object_dir=scratch.join("objects");config.create=false;
  let mirror=config.open_store()?;println!("{}",serde_json::json!({"prefix":mirror.prefix(),"verified_from_actual_store_copy":true}));return Ok(())
 }
 assert_eq!(args.len(),5);
 let out:Outcome=serde_json::from_slice(&fs::read(&args[1])?)?;
 assert_eq!(out.status,Status::Observed);assert!(!out.historical);
 let o=out.observation.as_ref().unwrap();let store=Path::new(&args[2]);let export=Path::new(&args[3]);let scratch=Path::new(&args[4]);
 fs::DirBuilder::new().mode(0o700).create(scratch)?;
 let mut total=0;let mut expected=BTreeSet::new();
 for root in &o.objects {let b=fs::read(store.join(format!("object-{}",hex(&root.digest))))?;root.verify(&b,64*1024*1024)?;total+=b.len();assert!(total<=64*1024*1024);assert!(expected.insert(root.digest));let p=scratch.join(format!("{}.bin",hex(&root.digest)));fs::write(&p,&b)?;fs::set_permissions(&p,fs::Permissions::from_mode(0o600))?;}
 let body=tickwire_ipc::HandoverReadyBody{instance_id:o.process.instance,logical_strategy_id:o.process.logical_strategy_id,strategy_generation:o.process.generation,exec_report_sequence:o.account_position,report_ring_sequence:o.report_ring_sequence,ledger_sequence:o.ledger_sequence,checkpoint_digest:o.checkpoint.digest,orders_digest:o.orders_digest,input_seal_digest:o.input_seal.digest,checkpoint_bytes:o.checkpoint.bytes,input_seal_bytes:o.input_seal.bytes,ready_sequence:o.ready_sequence};
 let (digest,objects)=tickwire_strategy_sdk::handover::objects(scratch,&body)?;
 assert_eq!(digest,o.account_input_digest);assert_eq!(objects.iter().map(|b|Object::of(b).digest).collect::<BTreeSet<_>>(),expected);
 let baseline=tickwire_account_codec::CanonicalBaseline::decode(&fs::read(export.join("baseline.cbor"))?).map_err(|e|format!("baseline {e:?}"))?;
 let mut seal=tickwire_account_codec::InputSeal::new(baseline.digest()).map_err(|e|format!("seal {e:?}"))?;
 for n in 1..=o.account_position {let r=tickwire_account_codec::CanonicalRecord::decode(&fs::read(export.join(format!("records/{n:020}.cbor")))?).map_err(|e|format!("row {e:?}"))?;seal.apply(&r).map_err(|e|format!("apply {e:?}"))?;}
 assert_eq!(seal.digest(),o.account_input_digest);
 println!("{}",serde_json::json!({"request_id":hex(&out.request.request_id),"objects":objects.len(),"bytes":total,"ready_sequence":o.ready_sequence,"account_position":o.account_position,"account_input_digest":hex(&digest),"checkpoint":hex(&o.checkpoint.digest),"input_seal":hex(&o.input_seal.digest),"closure_verified":true,"independent_export_prefix_verified":true}));Ok(())
}
