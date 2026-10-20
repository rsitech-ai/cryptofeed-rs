//! Opt-in public Hyperliquid source/projection capture and same-artifact audit.
use marketfeed_daemon::causal_bootstrap::{capture::*, current_build};
use std::{io::Read, path::Path};

fn read_request(path: &Path) -> Result<CaptureRequest, Box<dyn std::error::Error + Send + Sync>> {
    let mut bytes = Vec::new();
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err("capture request must be a regular file".into());
    }
    file.take(65_537).read_to_end(&mut bytes)?;
    if bytes.len() > 65_536 {
        return Err("capture request exceeds 64KiB".into());
    }
    let request: CaptureRequest = serde_json::from_slice(&bytes)?;
    request.validate()?;
    Ok(request)
}
async fn command() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let value=match args.as_slice() {
        [command] if command=="identity" => serde_json::to_value(current_build()?)?,
        [command, flag, root] if command=="verify" && flag=="--output" => verify(Path::new(root))?,
        [command, request_flag, request, output_flag, root] if command=="run" && request_flag=="--request" && output_flag=="--output" => run_public(read_request(Path::new(request))?, Path::new(root)).await?,
        _ => return Err("usage: marketfeed-capture identity | verify --output <capture-root> | run --request <json> --output <new-root>".into()),
    };
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}
#[tokio::main]
async fn main() {
    if let Err(error) = command().await {
        eprintln!("marketfeed-capture: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn request_bytes_and_unknown_fields_are_bounded_before_network() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("request");
        std::fs::write(&path, vec![b' '; 65_537]).unwrap();
        assert!(
            read_request(&path)
                .unwrap_err()
                .to_string()
                .contains("64KiB")
        );
        std::fs::write(&path,br#"{"schema_version":1,"markets":[],"duration_ms":1000,"source_max_bytes":67108864,"target_max_bytes":67108864,"extra":true}"#).unwrap();
        assert!(read_request(&path).is_err());
    }
}
