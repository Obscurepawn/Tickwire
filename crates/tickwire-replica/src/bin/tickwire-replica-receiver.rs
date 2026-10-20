use std::{thread, time::Duration};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os();
    let program = args.next().unwrap_or_default();
    let path = args
        .next()
        .ok_or_else(|| format!("usage: {} CONFIG.json", program.to_string_lossy()))?;
    if args.next().is_some() {
        return Err("expected one configuration path".into());
    }
    let mut listener = tickwire_replica::config::ReceiverConfig::from_file(path)?.open()?;
    println!("replica receiver listening on {}", listener.local_addr()?);
    loop {
        if listener.poll()? == 0 {
            thread::sleep(Duration::from_millis(1));
        }
    }
}
