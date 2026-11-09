Test-only TLS fixtures for tests/egress.rs: a CA (its key was discarded
after signing) and a leaf for DNS name `localhost` only, valid until 2126.
The leaf key is public on purpose; trust ca.pem nowhere but in tests.
