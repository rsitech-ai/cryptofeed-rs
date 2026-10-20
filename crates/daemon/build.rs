//! Ensure `ui/dist` exists when building with `--features ui`.
//!
//! Production embeds checked-in Vite/Svelte output (`ui/dist/assets/app.js`).
//! Rebuild with `cd ui && npm install && npm run build` when sources change.

use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

fn compiled_causal_binding() {
    let output =
        Path::new(&std::env::var_os("OUT_DIR").expect("OUT_DIR")).join("causal-build.json");
    if std::env::var_os("CARGO_FEATURE_CAUSAL_JOURNAL").is_none() {
        fs::write(output, b"null").expect("write disabled causal binding");
        return;
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert!(
        root.join("Cargo.lock").is_file() && root.join("crates").is_dir(),
        "causal-journal requires a complete workspace-source artifact recipe; standalone registry-package builds are unsupported for this opt-in feature"
    );
    let mut files = Vec::new();
    fn visit(root: &Path, path: &Path, files: &mut Vec<String>) {
        let metadata = fs::symlink_metadata(path).expect("stat build source");
        assert!(
            !metadata.file_type().is_symlink(),
            "symlink in source snapshot"
        );
        if metadata.is_dir() {
            for entry in fs::read_dir(path).expect("read source directory") {
                visit(root, &entry.expect("read source entry").path(), files);
            }
        } else if metadata.is_file() {
            files.push(
                path.strip_prefix(root)
                    .expect("workspace source")
                    .to_str()
                    .expect("UTF8 source path")
                    .replace('\\', "/"),
            );
        } else {
            panic!("unsupported source file type");
        }
    }
    for path in [
        "Cargo.toml",
        "Cargo.lock",
        "rust-toolchain.toml",
        "crates",
        "ui/dist",
        ".cargo",
    ] {
        let path = root.join(path);
        println!("cargo:rerun-if-changed={}", path.display());
        if path.exists() {
            visit(&root, &path, &mut files);
        }
    }
    files.sort();
    let mut source = Sha256::new();
    source.update(b"marketfeed-source-snapshot-v1\0");
    for name in &files {
        let bytes = fs::read(root.join(name)).expect("read build source");
        source.update((name.len() as u64).to_le_bytes());
        source.update(name.as_bytes());
        source.update((bytes.len() as u64).to_le_bytes());
        source.update(&bytes);
    }
    let rustc = std::process::Command::new(std::env::var_os("RUSTC").expect("RUSTC"))
        .arg("-vV")
        .output()
        .expect("observe compiler");
    assert!(rustc.status.success(), "compiler identity unavailable");
    let git_commit = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&root)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_owned());
    let mut features: Vec<_> = std::env::vars()
        .filter_map(|(name, _)| {
            name.strip_prefix("CARGO_FEATURE_")
                .map(|s| s.to_ascii_lowercase().replace('_', "-"))
        })
        .collect();
    features.sort();
    let recipe = serde_json::json!({
        "schema_version":1, "package_version":env!("CARGO_PKG_VERSION"),
        "source_scheme":"sha256-path-length-and-bytes-v1", "source_sha256":format!("{:x}",source.finalize()),
        "source_files":files.len(), "lock_sha256":format!("{:x}",Sha256::digest(fs::read(root.join("Cargo.lock")).expect("read build lock"))),
        "git_commit":git_commit, "rustc_verbose":String::from_utf8(rustc.stdout).expect("compiler UTF8"),
        "target":std::env::var("TARGET").expect("TARGET"), "profile":std::env::var("PROFILE").expect("PROFILE"),
        "opt_level":std::env::var("OPT_LEVEL").expect("OPT_LEVEL"), "debug":std::env::var("DEBUG").expect("DEBUG"),
        "encoded_rustflags":std::env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default(),
        "package_features":[{"package":"marketfeed-daemon","features":features}]
    });
    fs::write(
        Path::new(&std::env::var_os("OUT_DIR").expect("OUT_DIR")).join("causal-build.json"),
        serde_json::to_vec(&recipe).expect("encode compiled binding"),
    )
    .expect("save compiled binding");
}

fn main() {
    compiled_causal_binding();
    println!("cargo:rerun-if-changed=../../ui/dist");
    println!("cargo:rerun-if-changed=../../ui/dist/index.html");
    println!("cargo:rerun-if-changed=../../ui/dist/assets/app.js");
    println!("cargo:rerun-if-changed=../../ui/dist/assets/app.css");
    println!("cargo:rerun-if-changed=../../ui/package.json");

    if std::env::var("CARGO_FEATURE_UI").is_err() {
        return;
    }

    let dist = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ui/dist");
    let index = dist.join("index.html");
    let js = dist.join("assets/app.js");
    let css = dist.join("assets/app.css");

    for path in [&index, &js, &css] {
        if !path.is_file() {
            panic!(
                "missing {} — build the SPA first: `cd ui && npm install && npm run build`",
                path.display()
            );
        }
    }

    let js_bytes = fs::read(&js).expect("read ui/dist/assets/app.js");
    let js_text = String::from_utf8_lossy(&js_bytes);
    if js_text.contains("marketfeed ui: run npm run build") || js_bytes.len() < 10_000 {
        panic!(
            "ui/dist/assets/app.js looks like a placeholder stub ({} bytes). \
             Rebuild with: `cd ui && npm install && npm run build`",
            js_bytes.len()
        );
    }
    if !js_text.contains("svelte") && !js_text.contains("__svelte") {
        panic!(
            "ui/dist/assets/app.js does not look like a Svelte/Vite production build. \
             Rebuild with: `cd ui && npm install && npm run build`"
        );
    }
}
