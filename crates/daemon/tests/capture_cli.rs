//! Actual child preflight checks. No exchange or source directory is opened.
#[cfg(any(target_os = "macos", target_os = "linux"))]
#[test]
fn fifo_request_is_refused_without_blocking_or_creating_capture() {
    use std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    let temp = tempfile::tempdir().unwrap();
    let request = temp.path().join("request");
    let output = temp.path().join("output");
    assert!(
        Command::new("mkfifo")
            .arg(&request)
            .status()
            .unwrap()
            .success()
    );
    let mut child = Command::new(env!("CARGO_BIN_EXE_marketfeed-capture"))
        .args(["run", "--request"])
        .arg(&request)
        .arg("--output")
        .arg(&output)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let start = Instant::now();
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("FIFO request blocked before validation");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let result = child.wait_with_output().unwrap();
    assert_eq!(result.status.code(), Some(1));
    assert!(
        String::from_utf8(result.stderr)
            .unwrap()
            .contains("regular file")
    );
    assert!(!output.exists());
}
