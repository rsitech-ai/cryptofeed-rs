use std::{fs, process::Command};

#[test]
fn inspect_and_admit_never_create_store_resources_even_on_error() {
    let temp = tempfile::tempdir().unwrap();
    for command in ["inspect", "admit"] {
        for present in [false, true] {
            let path = temp.path().join(format!("{command}-{present}"));
            if present {
                fs::create_dir(&path).unwrap();
            }
            let mut child = Command::new(env!("CARGO_BIN_EXE_marketfeed-dataset"));
            child
                .args([command, "--store"])
                .arg(&path)
                .args(["--manifest", &"a".repeat(64)]);
            if command == "admit" {
                child.args(["--profile", "execution-count"]);
            }
            assert!(!child.output().unwrap().status.success());
            if present {
                assert_eq!(fs::read_dir(&path).unwrap().count(), 0);
            } else {
                assert!(!path.exists());
            }
        }
    }
}
