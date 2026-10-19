# Changelog

## 0.1.0

- Initial public release of the self-hosted Android push SDK.
- Receive notifications through Centrifuge and a user-enabled foreground service.
- Persist the inbox, deduplication, acknowledgements and notification opens in SQLite.
- Store notification credentials in protected storage and isolate account bindings.
- Support explicit start/stop, credential renewal, offline synchronization and revocation.
- Include an Android example and host integration instructions.
- Require Flutter 3.44+, Dart 3.12+ and Android API 24+.
