//! Ensure `ui/dist` exists when building with `--features ui`.
//!
//! Production embeds checked-in Vite/Svelte output (`ui/dist/assets/app.js`).
//! Rebuild with `cd ui && npm install && npm run build` when sources change.

use std::fs;
use std::path::Path;

fn main() {
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
