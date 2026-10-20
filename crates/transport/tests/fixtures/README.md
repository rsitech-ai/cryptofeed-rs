Synthetic localhost TLS handshake fixtures. The DER key is intentionally public,
generated only for these tests and unusable as a production credential. The
self-signed certificate has a localhost DNS SAN, CA:false and serverAuth usage.
Tests exercise normal trust/hostname verification with the private fixture root
only in the test connector; the production connector uses bundled public roots
and rejects this certificate. Never install the fixture in any system trust store.
