// Real Go/Dart wire smoke test. Run through ../tools/check_e2e.py.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:http/http.dart' as http;
import 'package:pushx_flutter/src/api.dart';
import 'package:pushx_flutter/src/config.dart';
import 'package:pushx_flutter/src/protocol.dart';
import 'package:pushx_flutter/src/transport.dart';

Future<void> main() async {
  var stage = 'registration';
  final client = http.Client();
  PushxCentrifugeTransport? transport;
  PushxCentrifugeTransport? replacedTransport;
  PushxHttpApi? api;
  final subscriptions = <StreamSubscription<dynamic>>[];
  try {
    final base = Uri.parse(Platform.environment['PUSHX_TEST_URL']!);
    final bootstrap = Platform.environment['PUSHX_DEMO_TOKEN']!;
    Future<Json> admin(String path, {bool get = false}) async {
      final request = http.Request(get ? 'GET' : 'POST', base.resolve(path))
        ..headers['Authorization'] = 'Bearer $bootstrap';
      final response = await http.Response.fromStream(
        await client.send(request),
      );
      if (response.statusCode != 200) throw StateError('admin request failed');
      return jsonObject(jsonDecode(response.body));
    }

    final registration = await admin('demo/register');
    final snapshot = PushxSnapshot(
      PushxConfig(hub: base, allowInsecure: true),
      PushxBinding(
        scope: jsonString(registration, 'scope'),
        subscriptionId: jsonString(registration, 'subscriptionId'),
        bindingId: jsonString(registration, 'bindingId'),
        credentialKey: 'test-memory-only',
        expiresAt: jsonInt(registration, 'expiresAt'),
      ),
      1,
      true,
    );
    final token = jsonString(registration, 'credential');
    api = PushxHttpApi(snapshot, token);
    stage = 'offline sync';
    final offline = await admin('demo/send');
    final page = await api.sync('');
    if (!page.packets.any(
      (p) => p['notificationId'] == offline['notificationId'],
    )) {
      throw StateError('offline message missing');
    }
    await api.acknowledge([
      PushxAck(jsonString(offline, 'notificationId'), PushxAckStage.posted),
    ]);
    stage = 'websocket and dispatcher';
    transport = PushxCentrifugeTransport(snapshot, token);
    final connected = Completer<void>(), denied = Completer<void>();
    final superseded = Completer<PushxConnectionEvent>();
    final packet = Completer<PushxPacket>();
    subscriptions.add(
      transport.states.listen((event) {
        if (event.state == 'connected' && !connected.isCompleted) {
          connected.complete();
        }
        if (event.denied && !denied.isCompleted) denied.complete();
        if (event.superseded && !superseded.isCompleted) {
          superseded.complete(event);
        }
      }),
    );
    subscriptions.add(
      transport.packets.listen((data) {
        if (!packet.isCompleted) packet.complete(PushxPacket.decode(data));
      }),
    );
    await transport.connect();
    await connected.future.timeout(const Duration(seconds: 8));
    stage = 'connection replacement';
    replacedTransport = transport;
    transport = PushxCentrifugeTransport(snapshot, token);
    final replacementConnected = Completer<void>();
    subscriptions.add(
      transport.states.listen((event) {
        if (event.state == 'connected' && !replacementConnected.isCompleted) {
          replacementConnected.complete();
        }
        if (event.denied && !denied.isCompleted) denied.complete();
      }),
    );
    subscriptions.add(
      transport.packets.listen((data) {
        if (!packet.isCompleted) packet.complete(PushxPacket.decode(data));
      }),
    );
    await transport.connect();
    await replacementConnected.future.timeout(const Duration(seconds: 8));
    final replaced = await superseded.future.timeout(
      const Duration(seconds: 8),
    );
    if (replaced.disconnectCode != 3501 ||
        replaced.denied ||
        denied.isCompleted) {
      throw StateError('replacement mistaken for revocation');
    }
    await replacedTransport.close();
    replacedTransport = null;
    final sent = await admin('demo/send');
    final received = await packet.future.timeout(const Duration(seconds: 8));
    if (received.notificationId != sent['notificationId'] ||
        received.bindingId != snapshot.binding.bindingId) {
      throw StateError('wire scope mismatch');
    }
    stage = 'acknowledgement';
    await api.acknowledge([
      PushxAck(received.notificationId, PushxAckStage.posted),
    ]);
    final state = await admin('demo/state', get: true);
    if (!(state['deliveries'] as List).any(
      (r) =>
          r['notificationId'] == received.notificationId &&
          r['state'] == 'posted',
    )) {
      throw StateError('ACK missing');
    }
    stage = 'renew/status/revoke';
    await api.renew();
    await api.report({
      'service': 'running',
      'connection': 'connected',
      'permission': 'granted',
    });
    await api.stop();
    await denied.future.timeout(const Duration(seconds: 8));
    var rejected = false;
    try {
      await api.sync('');
    } on PushxApiException catch (error) {
      rejected = error.unauthorized;
    }
    if (!rejected) throw StateError('revoked credential accepted');
    stdout.writeln(
      'PASS: Go dispatcher -> Dart transport -> ACK -> renew/status -> revoke; offline sync; replacement 3501',
    );
  } on Object {
    stderr.writeln('FAIL: protocol smoke at $stage');
    exitCode = 1;
  } finally {
    for (final subscription in subscriptions) {
      await subscription.cancel();
    }
    await transport?.close();
    await replacedTransport?.close();
    api?.close();
    client.close();
  }
}
