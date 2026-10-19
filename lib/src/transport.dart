import 'dart:async';
import 'dart:convert';

import 'package:centrifuge/centrifuge.dart' as centrifuge;

import 'config.dart';

class PushxConnectionEvent {
  const PushxConnectionEvent(
    this.state, {
    this.serverNow,
    this.disconnectCode,
    bool denied = false,
  }) : _denied =
           denied ||
           (disconnectCode != null &&
               disconnectCode >= 3500 &&
               disconnectCode < 4000);
  final String state;
  final int? serverNow;
  final int? disconnectCode;
  final bool _denied;

  /// 3501 ends only the superseded connection, not the binding authorization.
  bool get superseded => disconnectCode == 3501;
  bool get denied => !superseded && _denied;
}

abstract interface class PushxTransport {
  Stream<List<int>> get packets;
  Stream<PushxConnectionEvent> get states;
  Future<void> connect();
  Future<void> close();
}

/// Centrifuge alone owns the reconnect loop. No additional periodic connect()
/// calls are layered on top of its backoff and server-side recovery.
class PushxCentrifugeTransport implements PushxTransport {
  PushxCentrifugeTransport(PushxSnapshot snapshot, String token)
    : _client = centrifuge.createClient(
        snapshot.config
            .endpoint(snapshot.config.endpoints.websocket, websocket: true)
            .toString(),
        centrifuge.ClientConfig(
          token: token,
          name: 'pushx_flutter',
          version: '0.1.0',
          minReconnectDelay: const Duration(seconds: 1),
          maxReconnectDelay: const Duration(seconds: 30),
        ),
      ) {
    _subscriptions.add(_client.publication.listen((e) => _packets.add(e.data)));
    _subscriptions.add(
      _client.connecting.listen(
        (_) => _states.add(const PushxConnectionEvent('connecting')),
      ),
    );
    _subscriptions.add(
      _client.connected.listen((e) {
        int? serverNow;
        try {
          final data = jsonDecode(utf8.decode(e.data));
          if (data is Map && data['serverNow'] is int) {
            serverNow = data['serverNow'] as int;
          }
        } on Object {
          /* HTTP sync supplies authoritative time as well. */
        }
        _states.add(PushxConnectionEvent('connected', serverNow: serverNow));
      }),
    );
    _subscriptions.add(
      _client.disconnected.listen(
        (e) => _states.add(
          PushxConnectionEvent('disconnected', disconnectCode: e.code),
        ),
      ),
    );
    // Never forward third-party error strings: they may contain frame data.
    _subscriptions.add(
      _client.error.listen(
        (_) => _states.add(const PushxConnectionEvent('connecting')),
      ),
    );
  }
  final centrifuge.Client _client;
  final _packets = StreamController<List<int>>.broadcast();
  final _states = StreamController<PushxConnectionEvent>.broadcast();
  final List<StreamSubscription<dynamic>> _subscriptions = [];
  @override
  Stream<List<int>> get packets => _packets.stream;
  @override
  Stream<PushxConnectionEvent> get states => _states.stream;
  @override
  Future<void> connect() => _client.connect();
  @override
  Future<void> close() async {
    for (final subscription in _subscriptions) {
      await subscription.cancel();
    }
    await _client.close();
    await _packets.close();
    await _states.close();
  }
}
