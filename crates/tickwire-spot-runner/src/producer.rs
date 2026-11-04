//! A paper worker consumes the same quote independently and publishes the existing
//! FileStrategy envelope, actual input bytes and content-hashed decision dependencies.
use super::*;
use serde_json::json;
use tickwire_recovery::{InputRecord, digest, encode_inputs};
pub fn publish(venue: SpotVenue, root: &Path, id: u64) -> Result<()> {
    let owner = [id as u8; 16];
    let cmd = template(venue, id)?;
    let dir = root.join(id.to_string());
    fs::create_dir(&dir)?;
    let quote = fs::read(root.join("quote.json"))?;
    let q: serde_json::Value = serde_json::from_slice(&quote)?;
    let threshold = if id == 8 { 60000 } else { 60001 };
    let ask = q["ask"].as_u64().ok_or("missing paper quote")?;
    if q["symbol"] != cmd.command().context.instrument.symbol.as_str() || ask > threshold {
        return Err("paper strategy declined the observed quote".into());
    }
    let config =
        serde_json::to_vec(&json!({"strategy":id,"threshold":threshold,"profile":"local-paper"}))?;
    let inputs = encode_inputs(
        [id as u8; 16],
        1,
        [0; 32],
        &[
            InputRecord {
                kind: 4,
                payload: &config,
            },
            InputRecord {
                kind: 1,
                payload: &quote,
            },
        ],
    )?;
    let cpath = dir.join("command.cbor");
    fs::write(&cpath, cmd.bytes())?;
    let ipath = dir.join("decision.inputs");
    fs::write(&ipath, &inputs.bytes)?;
    let mut deps = vec![];
    for (kind, name) in [
        (2, "reservation"),
        (3, "dedup-prefix"),
        (4, "local-paper-action-grant"),
        (5, "strategy-parameters"),
        (6, "paper-catalog"),
        (7, "paper-run-operation"),
        (8, "strategy-member"),
        (9, "quote-input"),
        (10, "gate-policy"),
        (11, "decision-checkpoint"),
    ] {
        let value = serde_json::to_vec(
            &json!({"dependency":name,"member":id,"decision":cmd.digest(),"config_hash":digest(&config),"observed_quote_hash":digest(&quote)}),
        )?;
        let p = dir.join(name);
        fs::write(&p, &value)?;
        deps.push(
            json!({"kind":kind,"schema_id":1,"owner":owner,"path":p,"digest":digest(&value)}),
        );
    }
    let record = json!({"command":cpath,"command_digest":cmd.digest(),"order_ids":[owner],
        "recovery":{"dependencies":deps,"inputs":ipath,"input_digest":digest(&inputs.bytes),"declared_rebuild":false,
        "fault_model_id":digest(b"fresh single-host loopback paper exchange"),"pin_id":owner,"owner":owner}});
    let tmp = dir.join("descriptor.tmp");
    fs::write(&tmp, serde_json::to_vec(&record)?)?;
    fs::rename(tmp, dir.join("00000000000000000001.json"))?;
    Ok(())
}
