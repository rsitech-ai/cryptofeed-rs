use marketfeed_datasets::{
    Result,
    export::{self, ExportRequest},
    fail,
    store::Store,
};
use std::{collections::BTreeMap, path::Path};
fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let command = args
        .next()
        .ok_or_else(|| fail("command required: export|inspect|admit|recover|backup|restore"))?;
    let mut options = BTreeMap::new();
    while let Some(key) = args.next() {
        if !key.starts_with("--") {
            return Err(fail("expected named option"));
        }
        let value = args.next().ok_or_else(|| fail("option value required"))?;
        if options.insert(key, value).is_some() {
            return Err(fail("duplicate option"));
        }
    }
    let allowed = match command.as_str() {
        "export" => vec![
            "--store",
            "--feed",
            "--raw",
            "--dataset",
            "--start-ns",
            "--end-ns",
            "--region",
            "--expected-parent",
        ],
        "inspect" => vec!["--store", "--manifest"],
        "admit" => vec!["--store", "--manifest", "--profile"],
        "recover" => vec!["--store"],
        "backup" | "restore" => vec!["--store", "--manifest", "--destination"],
        _ => return Err(fail("unsupported command")),
    };
    if options.keys().any(|k| !allowed.contains(&k.as_str())) {
        return Err(fail("unknown option"));
    }
    let get = |key: &str| {
        options
            .get(key)
            .map(String::as_str)
            .ok_or_else(|| fail(&format!("missing {key}")))
    };
    let store = Store::open(get("--store")?)?;
    match command.as_str() {
        "export" => {
            let parent = get("--expected-parent")?;
            let pin = export::export(
                &store,
                &ExportRequest {
                    feed: Path::new(get("--feed")?),
                    raw: Path::new(get("--raw")?),
                    dataset: get("--dataset")?,
                    start_ns: get("--start-ns")?.parse()?,
                    end_ns: get("--end-ns")?.parse()?,
                    region: get("--region")?,
                    expected_parent: if parent == "none" { None } else { Some(parent) },
                },
            )?;
            println!(
                "{}",
                serde_json::json!({"manifest_sha256":pin,"decision":store.published_manifest(&pin)?["quality"]["decision"],"strict_profiles":[]})
            );
        }
        "inspect" => {
            let manifest = store.published_manifest(get("--manifest")?)?;
            let rows = export::verify_manifest(&store, &manifest)?;
            println!(
                "{}",
                serde_json::json!({"manifest":manifest,"verified_rows":rows.len(),"admitted":false})
            );
        }
        "admit" => {
            let rows = export::admit(&store, get("--manifest")?, get("--profile")?)?;
            println!("{}", serde_json::json!({"admitted_rows":rows.len()}));
        }
        "recover" => println!(
            "{}",
            serde_json::json!({"recovered_manifest":store.recover()?})
        ),
        "backup" | "restore" => {
            let pin = get("--manifest")?;
            export::verify_manifest(&store, &store.published_manifest(pin)?)?;
            let destination = Path::new(get("--destination")?);
            store.backup_to(pin, destination)?;
            let restored = Store::open(destination)?;
            export::verify_manifest(&restored, &restored.published_manifest(pin)?)?;
            println!(
                "{}",
                serde_json::json!({"verified_manifest_sha256":pin,"admitted":false})
            );
        }
        _ => unreachable!(),
    }
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("dataset operation failed: {error}");
        std::process::exit(1);
    }
}
