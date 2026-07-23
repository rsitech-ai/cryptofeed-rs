use base64::Engine;
use marketfeed_adapter_coinbase::{CoinbaseExchangeCredentials, CoinbaseExchangeCredentialsError};

#[test]
fn exchange_credentials_sign_coinbase_verify_prehash() {
    let secret = base64::engine::general_purpose::STANDARD.encode(b"secret");
    let credentials = CoinbaseExchangeCredentials::new("fixture-key", secret, "fixture-passphrase")
        .expect("synthetic credentials");

    let auth = credentials
        .sign_subscribe(1_700_000_000)
        .expect("synthetic signature");

    assert_eq!(auth.timestamp, "1700000000");
    assert_eq!(auth.key, "fixture-key");
    assert_eq!(auth.passphrase, "fixture-passphrase");
    assert_eq!(
        auth.signature,
        "lhmJXK08fk9SI1ZwFXKFRrPtzfbNOwC+D1xMJJ/1KZg="
    );
}

#[test]
fn exchange_credentials_reject_invalid_base64_secret_without_echoing_it() {
    let invalid_secret = "not-base64***";
    let error =
        CoinbaseExchangeCredentials::new("fixture-key", invalid_secret, "fixture-passphrase")
            .expect_err("invalid base64 must fail closed");

    assert_eq!(error, CoinbaseExchangeCredentialsError::InvalidBase64Secret);
    assert!(!error.to_string().contains(invalid_secret));
}

#[test]
fn exchange_credentials_debug_redacts_every_credential_field() {
    let secret = base64::engine::general_purpose::STANDARD.encode(b"super-secret");
    let credentials =
        CoinbaseExchangeCredentials::new("sensitive-key", secret.clone(), "sensitive-passphrase")
            .expect("synthetic credentials");

    let debug = format!("{credentials:?}");

    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("sensitive-key"));
    assert!(!debug.contains(&secret));
    assert!(!debug.contains("sensitive-passphrase"));
}

#[test]
fn exchange_subscribe_auth_debug_redacts_every_authentication_field() {
    let secret = base64::engine::general_purpose::STANDARD.encode(b"secret");
    let credentials =
        CoinbaseExchangeCredentials::new("sensitive-key", secret, "sensitive-passphrase")
            .expect("synthetic credentials");
    let auth = credentials
        .sign_subscribe(1_700_000_000)
        .expect("synthetic signature");

    let debug = format!("{auth:?}");

    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("sensitive-key"));
    assert!(!debug.contains("sensitive-passphrase"));
    assert!(!debug.contains("lhmJXK08"));
}

#[test]
fn exchange_credentials_reject_blank_fields() {
    let secret = base64::engine::general_purpose::STANDARD.encode(b"secret");

    assert_eq!(
        CoinbaseExchangeCredentials::new("", secret.clone(), "passphrase").unwrap_err(),
        CoinbaseExchangeCredentialsError::EmptyField("api key")
    );
    assert_eq!(
        CoinbaseExchangeCredentials::new("key", secret.clone(), "").unwrap_err(),
        CoinbaseExchangeCredentialsError::EmptyField("passphrase")
    );
    assert_eq!(
        CoinbaseExchangeCredentials::new("key", "", "passphrase").unwrap_err(),
        CoinbaseExchangeCredentialsError::EmptyField("api secret")
    );
}
