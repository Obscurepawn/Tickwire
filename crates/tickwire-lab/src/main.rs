use std::path::Path;
fn main() {
    if let Err(error) = execute() {
        eprintln!("lab: {error}");
        std::process::exit(1);
    }
}
fn execute() -> tickwire_lab::Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice(){
        [op,config,gate,binary] if op=="child-exec"=>tickwire_lab::launch::child_exec(Path::new(config),Path::new(gate),Path::new(binary)),
        [op,kind_flag,kind,flag,out,bins_flag,bins] if op=="generate"&&kind_flag=="--scenario"&&flag=="--out"&&bins_flag=="--bin-dir"=>{
            let s=match kind.as_str(){
                "spot-handover"=>tickwire_lab::generate::generate_handover(Path::new(out),Path::new(bins))?,
                "spot-fill"=>tickwire_lab::generate::generate(Path::new(out),Path::new(bins))?,
                _=>return Err("expected spot-fill or spot-handover".into()),
            };
            println!("{}",serde_json::to_string_pretty(&s)?);Ok(())
        },
        [op,flag,out,bins_flag,bins] if op=="generate"&&flag=="--out"&&bins_flag=="--bin-dir"=>{
            let s=tickwire_lab::generate::generate(Path::new(out),Path::new(bins))?;
            println!("{}",serde_json::to_string_pretty(&s)?);Ok(())
        },
        [op,flag,path] if flag=="--config"=>{
            let s=tickwire_lab::Scenario::read(Path::new(path))?;
            match op.as_str(){"serve"=>tickwire_lab::fixture::serve(&s),"run"=>tickwire_lab::launch::run(&s),_=>Err("expected serve or run".into())}
        },
        _=>Err("usage: tickwire-lab generate [--scenario spot-fill|spot-handover] --out /new/directory --bin-dir /target/debug | serve|run --config /scenario.json".into()),
    }
}
