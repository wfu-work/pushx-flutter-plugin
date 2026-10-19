import 'dart:async';
import 'dart:convert';
import 'package:pushx_flutter/pushx_flutter.dart';

const testNow = 1700000000000;
const testCredential = 'pushx-test-notification-credential';

PushxPacket packet({
  String id = 'delivery-1',
  String event = 'event:1',
  String binding = 'binding-1',
  int? expires,
}) => PushxPacket(
  kind: 'business',
  notificationId: id,
  subscriptionId: 'subscription-1',
  bindingId: binding,
  eventKey: event,
  expiresAt: expires ?? testNow + 3600000,
  presentation: const PushxPresentation(title: 'Title', body: 'Body'),
  payload: const {'type': 'example'},
);
PushxConfig config() =>
    PushxConfig(hub: Uri.parse('https://example.test/prefix/api/'));
PushxBinding binding({
  String id = 'binding-1',
  int expires = testNow + 7 * 86400000,
}) => PushxBinding(
  scope: 'owner-1',
  subscriptionId: 'subscription-1',
  bindingId: id,
  credentialKey: 'test-key',
  expiresAt: expires,
);

class MemoryCredentials implements PushxCredentialStore {
  final values = <String, String>{'test-key': testCredential};
  bool failRead = false, failDelete = false;
  @override
  Future<String?> read(String key) async {
    if (failRead) throw StateError('read unavailable');
    return values[key];
  }

  @override
  Future<void> write(String key, String token) async {
    values[key] = token;
  }

  @override
  Future<void> delete(String key) async {
    if (failDelete) throw StateError('delete unavailable');
    values.remove(key);
  }
}

class FakeTransport implements PushxTransport {
  final publications = StreamController<List<int>>.broadcast(sync: true);
  final connections = StreamController<PushxConnectionEvent>.broadcast(
    sync: true,
  );
  bool closed = false;
  @override
  Stream<List<int>> get packets => publications.stream;
  @override
  Stream<PushxConnectionEvent> get states => connections.stream;
  @override
  Future<void> connect() async {
    connections.add(
      const PushxConnectionEvent('connected', serverNow: testNow),
    );
  }

  void send(PushxPacket p) =>
      publications.add(utf8.encode(jsonEncode(p.toJson())));
  @override
  Future<void> close() async {
    closed = true;
  }

  Future<void> dispose() async {
    await publications.close();
    await connections.close();
  }
}

class FakePresenter implements PushxPresenter {
  final shown = <int>[];
  final silenced = <bool>[];
  final canceled = <int>[];
  String blocked = '';
  Future<void> Function()? beforeShow;
  Future<void> Function()? beforeInitialize;
  Future<void> Function()? beforeCancel;
  @override
  Future<void> initialize(PushxConfig config) async =>
      await beforeInitialize?.call();
  @override
  Future<String> permission() async => 'granted';
  @override
  Future<bool> requestPermission() async => true;
  @override
  Future<String> show(
    PushxSnapshot s,
    PushxInboxItem item, {
    bool silent = false,
  }) async {
    shown.add(item.systemId);
    silenced.add(silent);
    await beforeShow?.call();
    return blocked;
  }

  @override
  Future<void> cancel(int systemId) async {
    await beforeCancel?.call();
    canceled.add(systemId);
  }
}

class FakeApi implements PushxApi {
  bool failAck = false, deny = false;
  final acknowledged = <PushxAck>[];
  final cursors = <String>[];
  int now = testNow;
  int renewCalls = 0, ackCalls = 0, stopCalls = 0, closeCalls = 0;
  Future<PushxSyncPage> Function(String cursor)? onSync;
  Future<PushxRenewal> Function()? onRenew;
  Future<void> Function()? onStop;
  @override
  Future<PushxSyncPage> sync(String cursor) async {
    cursors.add(cursor);
    if (deny) {
      throw const PushxApiException(
        'authorization_revoked',
        unauthorized: true,
      );
    }
    return onSync?.call(cursor) ?? PushxSyncPage([], '', false, now);
  }

  Future<void> Function(List<PushxAck>)? onAcknowledge;
  @override
  Future<void> acknowledge(List<PushxAck> acks) async {
    ackCalls++;
    await onAcknowledge?.call(acks);
    if (failAck) throw const PushxApiException('network_unavailable');
    acknowledged.addAll(acks);
  }

  @override
  Future<PushxRenewal> renew() async {
    renewCalls++;
    return onRenew?.call() ?? PushxRenewal(now + 7 * 86400000, now);
  }

  @override
  Future<void> report(Json status) async {}
  @override
  Future<void> stop() async {
    stopCalls++;
    await onStop?.call();
  }

  @override
  void close() {
    closeCalls++;
  }
}

class FakePlatform implements PushxPlatform {
  bool active = false;
  bool failStop = false;
  int initializations = 0, starts = 0, stops = 0, disposals = 0;
  Future<void> Function()? onInitialize;
  @override
  bool get supported => true;
  @override
  bool get visible => true;
  @override
  Future<bool> get running async => active;
  @override
  Future<void> initialize(
    String path,
    void Function() entry,
    void Function(Object) callback,
  ) async {
    initializations++;
    await onInitialize?.call();
  }

  @override
  Future<void> start(PushxConfig config, void Function() entry) async {
    active = true;
    starts++;
  }

  @override
  Future<void> stop() async {
    stops++;
    if (failStop) throw StateError('stop unavailable');
    active = false;
  }

  @override
  void dispose(void Function(Object) callback) {
    disposals++;
  }
}

Future<void> settle(PushxRuntime runtime) async {
  for (var i = 0; i < 4; i++) {
    await Future<void>.delayed(Duration.zero);
    await runtime.idle;
  }
}
