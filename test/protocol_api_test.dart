import 'dart:convert';
import 'dart:async';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:pushx_flutter/pushx_flutter.dart';

import 'support.dart';

class _WaitingClient extends http.BaseClient {
  final aborted = Completer<void>();
  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    await (request as http.AbortableRequest).abortTrigger;
    aborted.complete();
    throw http.RequestAbortedException(request.url);
  }
}

void main() {
  test('timed-out HTTP requests actually abort their transport', () async {
    final client = _WaitingClient();
    final api = PushxHttpApi(
      PushxSnapshot(config(), binding(), 1, true),
      testCredential,
      client: client,
      timeout: const Duration(milliseconds: 20),
    );
    await expectLater(
      api.sync(''),
      throwsA(
        isA<PushxApiException>().having(
          (e) => e.code,
          'code',
          'request_timeout',
        ),
      ),
    );
    await client.aborted.future;
    api.close();
  });
  test('shared Go/Dart protocol fixture and strict fields', () async {
    final bytes = await File('test/fixtures/business-v1.json').readAsBytes();
    final value = PushxPacket.decode(bytes);
    expect(value.notificationId, 'delivery-1');
    expect(
      () => PushxPacket.fromJson({...value.toJson(), 'extra': true}),
      throwsA(isA<PushxProtocolException>()),
    );
    expect(
      () => value.validate(now: value.expiresAt),
      throwsA(isA<PushxProtocolException>()),
    );
    expect(
      () =>
          PushxAck('id', PushxAckStage.blocked, reason: 'unbounded').validate(),
      throwsA(isA<PushxProtocolException>()),
    );
  });

  test(
    'API preserves Hub prefix and only carries notification credentials',
    () async {
      final s = PushxSnapshot(config(), binding(), 1, true);
      final api = PushxHttpApi(
        s,
        testCredential,
        client: MockClient((request) async {
          expect(request.url.path, '/prefix/api/mobile/notifications/ack');
          expect(request.headers['Authorization'], 'Bearer $testCredential');
          expect(jsonDecode(request.body)['bindingId'], 'binding-1');
          return http.Response('{"ok":true}', 200);
        }),
      );
      await api.acknowledge([
        const PushxAck('delivery-1', PushxAckStage.posted),
      ]);
      api.close();
    },
  );

  test('API refuses redirects and treats authorization separately', () async {
    final s = PushxSnapshot(config(), binding(), 1, true);
    for (final code in [302, 401]) {
      final api = PushxHttpApi(
        s,
        testCredential,
        client: MockClient((_) async => http.Response('', code)),
      );
      await expectLater(
        api.sync(''),
        throwsA(
          isA<PushxApiException>().having(
            (e) => e.unauthorized,
            'unauthorized',
            code == 401,
          ),
        ),
      );
      api.close();
    }
    expect(
      () => PushxConfig(
        hub: Uri.parse('https://example.test/api/'),
        endpoints: const PushxEndpoints(ack: 'https://other.test/ack'),
      ),
      throwsArgumentError,
    );
    expect(
      () => PushxConfig(hub: Uri.parse('http://example.test/')),
      throwsArgumentError,
    );
  });
}
