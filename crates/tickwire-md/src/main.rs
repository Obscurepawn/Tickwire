use std::path::Path;
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
fn run() -> tickwire_md::Result<()> {
    use tickwire_md::{config, invalid};
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [mode, path] if mode == "--config" => {
            let c = serde_json::from_slice(&config::read(Path::new(path), 4 * 1024 * 1024)?)?;
            tickwire_md::engine::run(c)
        }
        [mode, path, output, dest] if mode == "--resource" && output == "--output" => {
            let c: config::ResourceSpec =
                serde_json::from_slice(&config::read(Path::new(path), 1024 * 1024)?)?;
            let bytes = tickwire_market_source::protocol::wire_bytes(&c.header()?)?;
            config::write_bytes(Path::new(dest), &bytes)
        }
        [mode, path, output, dest] if mode == "--client" && output == "--output" => {
            let c: config::ClientTemplate =
                serde_json::from_slice(&config::read(Path::new(path), 1024 * 1024)?)?;
            config::write(Path::new(dest), &c.build()?)
        }
        [mode, path, dir, dest] if mode == "--admit" && dir == "--directory" => {
            let a = serde_json::from_slice(&config::read(Path::new(path), 2 * 1024 * 1024)?)?;
            tickwire_md::server::register_admission(Path::new(dest), &a)?;
            Ok(())
        }
        _ => Err(invalid(
            "usage: tickwire-md --config FILE | --resource FILE --output HEADER | --client FILE --output JSON | --admit FILE --directory DIR",
        )),
    }
}
