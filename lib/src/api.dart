import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;

import 'config.dart';
import 'protocol.dart';

class PushxApiException implements Exception {
  const PushxApiException(this.code, {this.unauthorized = false});
  final String code;
  final bool unauthorized;
  @override
  String toString() => 'PushxApiException($code)';
}

class PushxSyncPage {
  const PushxSyncPage(this.packets, this.cursor, this.hasMore, this.serverNow);
  final List<Json> packets;
  final String cursor;
  final bool hasMore;
  final int serverNow;
}

class PushxRenewal {
  const PushxRenewal(this.expiresAt, this.serverNow);
  final int expiresAt, serverNow;
}

abstract interface class PushxApi {
  Future<PushxSyncPage> sync(String cursor);
  Future<void> acknowledge(List<PushxAck> acks);
  Future<PushxRenewal> renew();
  Future<void> report(Json status);
  Future<void> stop();
  void close();
}

/// Optional adapter for a host's API response wrapper. It must not access UI or
/// account refresh state; construct the same decoder in the background entrypoint.
typedef PushxResponseDecoder = Json Function(Json response);

class PushxHttpApi implements PushxApi {
  PushxHttpApi(
    this.snapshot,
    this.token, {
    http.Client? client,
    this.decoder,
    this.timeout = const Duration(seconds: 10),
  }) : _client = client ?? http.Client() {
    if (timeout <= Duration.zero) {
      throw ArgumentError('Positive HTTP timeout required');
    }
  }
  final PushxSnapshot snapshot;
  final String token;
  final PushxResponseDecoder? decoder;
  final http.Client _client;
  final Duration timeout;

  Future<Json> _request(
    String endpoint, {
    Json? body,
    Map<String, String>? query,
  }) async {
    var uri = snapshot.config.endpoint(endpoint);
    if (query != null) uri = uri.replace(queryParameters: query);
    final abort = Completer<void>();
    final request =
        http.AbortableRequest(
            body == null ? 'GET' : 'POST',
            uri,
            abortTrigger: abort.future,
          )
          ..followRedirects = false
          ..headers.addAll({
            'Authorization': 'Bearer $token',
            'Accept': 'application/json',
          });
    if (body != null) {
      request.headers['Content-Type'] = 'application/json';
      request.body = jsonEncode(body);
    }
    try {
      return await (() async {
        final response = await _client.send(request);
        if (response.statusCode == 401 || response.statusCode == 403) {
          throw const PushxApiException(
            'authorization_revoked',
            unauthorized: true,
          );
        }
        if (response.statusCode < 200 || response.statusCode >= 300) {
          throw const PushxApiException('http_failure');
        }
        final bytes = <int>[];
        await for (final chunk in response.stream) {
          if (bytes.length + chunk.length > 2 * 1024 * 1024) {
            throw const PushxApiException('response_too_large');
          }
          bytes.addAll(chunk);
        }
        if (bytes.isEmpty) return <String, dynamic>{};
        final result = jsonObject(jsonDecode(utf8.decode(bytes)));
        return decoder?.call(result) ?? result;
      })().timeout(timeout);
    } on TimeoutException {
      throw const PushxApiException('request_timeout');
    } on http.ClientException {
      throw const PushxApiException('network_unavailable');
    } finally {
      // Also release unread error bodies and stalled response streams.
      if (!abort.isCompleted) abort.complete();
    }
  }

  @override
  Future<PushxSyncPage> sync(String cursor) async {
    final j = await _request(
      snapshot.config.endpoints.sync,
      query: {'cursor': cursor, 'limit': '$pushxMaxBatchSize'},
    );
    final packets = j['packets'];
    final next = jsonString(j, 'cursor');
    if (packets is! List ||
        packets.length > pushxMaxBatchSize ||
        j['hasMore'] is! bool ||
        next.length > 1024) {
      throw const PushxApiException('invalid_sync_response');
    }
    return PushxSyncPage(
      packets.map(jsonObject).toList(),
      next,
      j['hasMore'] as bool,
      jsonInt(j, 'serverNow'),
    );
  }

  @override
  Future<void> acknowledge(List<PushxAck> acks) async {
    if (acks.isEmpty || acks.length > pushxMaxBatchSize) {
      throw ArgumentError('Invalid ACK batch');
    }
    await _request(
      snapshot.config.endpoints.ack,
      body: {
        'bindingId': snapshot.binding.bindingId,
        'acks': acks.map((a) => a.toJson()).toList(),
      },
    );
  }

  @override
  Future<PushxRenewal> renew() async {
    final j = await _request(
      snapshot.config.endpoints.renew,
      body: {'bindingId': snapshot.binding.bindingId},
    );
    return PushxRenewal(jsonInt(j, 'expiresAt'), jsonInt(j, 'serverNow'));
  }

  @override
  Future<void> report(Json status) async {
    await _request(
      snapshot.config.endpoints.status,
      body: {
        'bindingId': snapshot.binding.bindingId,
        'service': status['service'],
        'connection': status['connection'],
        'permission': status['permission'],
      },
    );
  }

  @override
  Future<void> stop() async {
    await _request(
      snapshot.config.endpoints.stop,
      body: {'bindingId': snapshot.binding.bindingId},
    );
  }

  @override
  void close() => _client.close();
}
