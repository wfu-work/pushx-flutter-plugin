import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'api.dart';
import 'config.dart';
import 'credentials.dart';
import 'lifecycle.dart';
import 'presenter.dart';
import 'protocol.dart';
import 'store.dart';
import 'transport.dart';

typedef PushxApiFactory =
    PushxApi Function(PushxSnapshot snapshot, String token);
typedef PushxTransportFactory =
    PushxTransport Function(PushxSnapshot snapshot, String token);

class _Cancelled implements Exception {}

class _Schedule {
  int next = 0, failures = 0;
}

/// Serializes durable changes while independently scheduling renewal, sync,
/// presentation and ACK. Stop/close interrupt the queue and abort network I/O.
class PushxRuntime {
  PushxRuntime({
    required this.store,
    required this.credentials,
    required this.presenter,
    this.codec = const PushxJsonPayloadCodec(),
    PushxApiFactory? apiFactory,
    PushxTransportFactory? transportFactory,
    int Function()? clock,
    double Function()? jitter,
    this.onStatus,
    this.onStopped,
  }) : _apiFactory = apiFactory ?? ((s, t) => PushxHttpApi(s, t)),
       _transportFactory =
           transportFactory ?? ((s, t) => PushxCentrifugeTransport(s, t)),
       _clock = clock ?? (() => DateTime.now().millisecondsSinceEpoch),
       _jitter = jitter ?? Random.secure().nextDouble,
       _owner = List.generate(
         16,
         (_) => Random.secure().nextInt(256).toRadixString(16).padLeft(2, '0'),
       ).join();

  final PushxStore store;
  final PushxCredentialStore credentials;
  final PushxPresenter presenter;
  final PushxPayloadCodec codec;
  final PushxApiFactory _apiFactory;
  final PushxTransportFactory _transportFactory;
  final int Function() _clock;
  final double Function() _jitter;
  final void Function(Json status)? onStatus;
  final FutureOr<void> Function()? onStopped;
  final String _owner;
  final _aborters = <Object, void Function()>{};
  final _jobs = <String, _Schedule>{};
  final _errors = <String, String>{};
  PushxSnapshot? _snapshot;
  PushxApi? _api;
  PushxTransport? _transport;
  final List<StreamSubscription<dynamic>> _subscriptions = [];
  Timer? _timer;
  Future<void> _tail = Future.value();
  Future<void>? _tickFuture, _closeFuture;
  Future<PushxStopResult>? _stopFuture;
  PushxConnectionEvent? _latestConnection;
  bool _closed = false,
      _stopping = false,
      _stateQueued = false,
      _notified = false;
  int _queued = 0, _offset = 0;
  String _connection = 'connecting';
  int get _now => _clock() + _offset;
  bool get _inactive => _closed || _stopping;
  _Schedule _schedule(String name) => _jobs.putIfAbsent(name, _Schedule.new);

  Future<T> _guard<T>(Future<T> work) {
    final result = Completer<T>();
    final key = Object();
    void cancel() {
      if (!result.isCompleted) result.completeError(_Cancelled());
    }

    _aborters[key] = cancel;
    work.then(
      (value) {
        _aborters.remove(key);
        if (!result.isCompleted) result.complete(value);
      },
      onError: (Object error, StackTrace stack) {
        _aborters.remove(key);
        if (!result.isCompleted) result.completeError(error, stack);
      },
    );
    if (_inactive) cancel();
    return result.future;
  }

  Future<void> start({bool periodic = true}) async {
    if (_snapshot != null || _inactive) {
      throw StateError('Runtime already started or closed');
    }
    final s = await store.claimRuntime(_owner);
    final status = await store.status();
    _offset = status['serverOffset'] is int ? status['serverOffset'] as int : 0;
    await _install(s);
    if (periodic && !_inactive) {
      _timer = Timer.periodic(
        const Duration(seconds: 15),
        (_) => unawaited(tick()),
      );
    }
  }

  Future<void> _install(PushxSnapshot s) async {
    await _closeTransport();
    if (_inactive) return;
    if (s.runtimeOwner != _owner) {
      await _retire();
      return;
    }
    if (!await store.isCurrent(s)) {
      await _refreshInstallation();
      return;
    }
    _snapshot = s;
    _jobs.clear();
    _errors.clear();
    _connection = 'connecting';
    final token = await credentials.read(s.binding.credentialKey);
    if (_inactive) return;
    if (!await store.isCurrent(s)) {
      await _refreshInstallation();
      return;
    }
    if (token == null || token.length < 16 || token.length > 2048) {
      await stopBinding();
      return;
    }
    await _guard(presenter.initialize(s.config));
    if (_inactive || !await store.isCurrent(s)) {
      if (!_inactive) await _refreshInstallation();
      return;
    }
    _api = _apiFactory(s, token);
    final transport = _transportFactory(s, token);
    _transport = transport;
    _subscriptions.add(
      transport.packets.listen((bytes) {
        if (bytes.length > pushxMaxPacketBytes) {
          _schedule('sync').next = 0;
          return;
        }
        unawaited(
          _enqueue(() async {
            if (_transport != transport || _inactive) return;
            await _acceptBytes(s, bytes);
            await _job('present', s, () => _present(s));
            await _job('ack', s, () => _flush(s));
          }),
        );
      }),
    );
    _subscriptions.add(
      transport.states.listen((event) {
        if (_transport != transport || _inactive) return;
        if (event.superseded) {
          unawaited(
            _handleSuperseded(transport).catchError((Object _) {
              _errors['connection'] = 'connection_unavailable';
            }),
          );
          return;
        }
        if (event.denied) {
          unawaited(
            stopBinding(revoked: true).then<void>(
              (_) {},
              onError: (Object _) {
                _errors['cleanup'] = 'cleanup_failure';
              },
            ),
          );
          return;
        }
        _latestConnection = event;
        _queueConnection();
      }),
    );
    try {
      await _guard(transport.connect());
    } on _Cancelled {
      return;
    } on Object {
      _errors['connection'] = 'connection_unavailable';
    }
    unawaited(tick());
  }

  Future<void> _refreshInstallation() async {
    final current = await store.snapshot();
    if (current == null || !current.enabled || current.runtimeOwner != _owner) {
      await _retire();
    } else {
      // Preference changes do not supersede the engine itself.
      await _install(current);
    }
  }

  Future<void> _handleSuperseded(PushxTransport transport) async {
    final current = await store.snapshot();
    if (_inactive || _transport != transport) return;
    if (current == null || !current.enabled || current.runtimeOwner != _owner) {
      await _retire();
    } else {
      // An older connect() can reach the server after the newer engine's
      // connection. Only the durable owner is allowed to reclaim the socket.
      await _enqueue(_refreshInstallation);
    }
  }

  void _queueConnection() {
    if (_stateQueued || _inactive || _queued >= 200) return;
    _stateQueued = true;
    unawaited(
      _enqueue(() async {
        final event = _latestConnection;
        _latestConnection = null;
        if (event == null || _inactive) return;
        _connection = event.state;
        if (event.serverNow != null) _updateTime(event.serverNow!);
        if (event.state == 'connected') {
          _errors.remove('connection');
          _schedule('sync').next = 0;
        }
        await _tick();
      }).whenComplete(() {
        _stateQueued = false;
        if (_latestConnection != null && !_inactive) _queueConnection();
      }),
    );
  }

  Future<void> _enqueue(Future<void> Function() work) {
    if (_inactive) return Future.value();
    if (_queued >= 200) {
      _schedule('sync').next = 0;
      return Future.value();
    }
    _queued++;
    final result = _tail.then((_) async {
      try {
        if (!_inactive) await work();
      } on _Cancelled {
        /* close/stop owns cleanup */
      } on PushxApiException catch (e) {
        _errors['runtime'] = e.code;
        if (e.unauthorized && !_inactive) await stopBinding(revoked: true);
      } on Object {
        _errors['runtime'] = 'runtime_failure';
      } finally {
        _queued--;
        final s = _snapshot;
        if (!_inactive && s != null && _transport != null) {
          try {
            await _status(s);
          } on Object {
            /* next tick */
          }
        }
        if (_latestConnection != null && !_stateQueued) _queueConnection();
      }
    });
    _tail = result.catchError((Object _) {
      _errors['cleanup'] = 'cleanup_failure';
    });
    return _tail;
  }

  /// Repeated ticks share one queued/in-flight operation, even on a slow network.
  Future<void> tick() {
    if (_inactive) return Future.value();
    if (_tickFuture != null) return _tickFuture!;
    return _tickFuture = _enqueue(_tick).whenComplete(() {
      _tickFuture = null;
    });
  }

  Future<void> _job(
    String name,
    PushxSnapshot s,
    Future<void> Function() work, {
    int Function()? nextDelay,
  }) async {
    if (_inactive ||
        _api == null ||
        s.runtimeOwner != _owner ||
        !await store.isCurrent(s)) {
      return;
    }
    final job = _schedule(name);
    if (_now < job.next) return;
    try {
      await _guard(work());
      if (_inactive) return;
      job.failures = 0;
      job.next = _now + (nextDelay?.call() ?? 0);
      _errors.remove(name);
    } on _Cancelled {
      return;
    } on Object catch (e) {
      if (_inactive) return;
      if (e is PushxApiException && e.unauthorized) {
        await stopBinding(revoked: true);
        return;
      }
      _errors[name] = e is PushxApiException
          ? e.code
          : e is PushxProtocolException
          ? e.code
          : '${name}_failed';
      job.failures = min(job.failures + 1, 8);
      final base = min(300000, 15000 * (1 << (job.failures - 1)));
      job.next = _now + (base * (0.8 + _jitter().clamp(0, 1) * 0.4)).round();
    }
  }

  Future<void> _tick() async {
    if (_inactive) return;
    var s = await store.snapshot();
    if (s != null && s.runtimeOwner != _owner) {
      await _retire();
      return;
    }
    if (s == null || !s.enabled) {
      await _closeTransport();
      _notifyStopped();
      return;
    }
    if (_snapshot?.revision != s.revision) {
      await _install(s);
      return;
    }
    // Expiry and renewal precede other network work. A broken sync/ACK endpoint
    // cannot suppress renewal, nor can a failed renewal suppress local receipt.
    if (s.binding.expiresAt <= _now) {
      await stopBinding();
      return;
    }
    if (s.binding.expiresAt - _now < const Duration(days: 1).inMilliseconds) {
      var delay = const Duration(hours: 1).inMilliseconds;
      final expected = s;
      await _job('renew', expected, () async {
        final renewal = await _guard(_api!.renew());
        _updateTime(renewal.serverNow);
        if (renewal.expiresAt <= _now) {
          throw const PushxApiException('invalid_renewal');
        }
        await store.extendCredential(expected, renewal.expiresAt);
        delay = min(delay, max(1000, (renewal.expiresAt - _now) ~/ 2));
      }, nextDelay: () => delay);
      if (_inactive) return;
      s = await store.snapshot();
      if (s == null || !s.enabled) return;
      if (s.runtimeOwner != _owner) {
        await _retire();
        return;
      }
    }
    final active = s;
    await _job(
      'prune',
      active,
      () => store.prune(_now, retentionDays: active.config.retentionDays),
      nextDelay: () => 300000,
    );
    await _job('present', active, () => _present(active));
    var complete = false;
    await _job('sync', active, () async {
      complete = await _sync(active);
    }, nextDelay: () => complete ? 300000 : 15000);
    await _job('present', active, () => _present(active));
    await _job('ack', active, () => _flush(active));
    if (_inactive) return;
    await _status(active);
    await _job(
      'report',
      active,
      () => _api!.report(_storeStatus()),
      nextDelay: () => 60000,
    );
  }

  Json _storeStatus() => {
    'service': 'running',
    'connection': _connection,
    'permission': _permission,
  };
  String _permission = 'unknown';

  void _updateTime(int serverNow) {
    if (serverNow <= 0 || serverNow > 9007199254740991) {
      throw const PushxApiException('invalid_server_time');
    }
    _offset = serverNow - _clock();
  }

  Future<bool> _sync(PushxSnapshot s) async {
    var cursor = '';
    final seen = <String>{};
    // Bound one round so presentation, ACK, status and stop stay responsive.
    for (var page = 0; page < 4; page++) {
      if (_inactive || !await store.isCurrent(s)) return false;
      final response = await _guard(_api!.sync(cursor));
      _updateTime(response.serverNow);
      for (final packet in response.packets) {
        if (_inactive) return false;
        await _acceptBytes(s, utf8.encode(jsonEncode(packet)));
      }
      await _job('present', s, () => _present(s));
      await _job('ack', s, () => _flush(s));
      if (!response.hasMore) return true;
      if (response.cursor.isEmpty || !seen.add(response.cursor)) {
        throw const PushxApiException('invalid_sync_cursor');
      }
      cursor = response.cursor;
    }
    return false;
  }

  Future<void> _acceptBytes(PushxSnapshot s, List<int> bytes) async {
    PushxPacket packet;
    try {
      packet = PushxPacket.decode(bytes);
    } on Object catch (error) {
      final reason =
          error is PushxProtocolException && error.code == 'unsupported_version'
          ? 'unsupported_version'
          : 'payload_invalid';
      _errors['protocol'] = reason;
      try {
        final raw = jsonObject(jsonDecode(utf8.decode(bytes)));
        final id = raw['notificationId'];
        if (raw['bindingId'] == s.binding.bindingId &&
            raw['subscriptionId'] == s.binding.subscriptionId &&
            id is String &&
            pushxIdentifier(id)) {
          await store.reject(s, id, reason);
        }
      } on Object {
        /* Unidentifiable input cannot be acknowledged. */
      }
      return;
    }
    if (packet.subscriptionId != s.binding.subscriptionId ||
        packet.bindingId != s.binding.bindingId) {
      return;
    }
    String? reason;
    try {
      codec.validate(packet);
    } on Object {
      reason = 'payload_invalid';
    }
    await store.accept(s, packet, _now, blockedReason: reason);
  }

  Future<void> _present(PushxSnapshot s) async {
    final active = await store.snapshot();
    if (active == null || active.binding.expiresAt <= _now) return;
    for (final item in await store.pending(s, _now)) {
      if (_inactive || !await store.claim(s, item, _owner, _now)) return;
      if (_inactive || !await store.isCurrent(s)) return;
      var reason = item.packet.expiresAt <= _now ? 'delivery_expired' : '';
      if (reason.isEmpty) {
        final submitted = presenter
            .show(s, item, silent: item.state == 'presenting')
            .then((r) async {
              // The OS call itself is not cancellable. A late old engine must
              // not cancel an ID now used by the live binding's newer engine.
              if (_inactive || !await store.isCurrent(s)) {
                await _cancelRetiredNotification(s, item.systemId);
              }
              return r;
            });
        reason = await _guard(submitted);
      }
      if (_inactive) return;
      if (reason.isEmpty && item.packet.expiresAt <= _now) {
        reason = 'delivery_expired';
      }
      final current = await store.finish(s, item, _owner, reason: reason);
      if (!current) {
        await _cancelRetiredNotification(s, item.systemId);
      } else if (reason == 'delivery_expired') {
        await presenter.cancel(item.systemId);
      }
    }
  }

  Future<void> _cancelRetiredNotification(PushxSnapshot s, int systemId) async {
    final current = await store.snapshotForCleanup();
    if (current != null &&
        current.enabled &&
        current.binding.scope == s.binding.scope &&
        current.binding.bindingId == s.binding.bindingId) {
      return;
    }
    await presenter.cancel(systemId);
  }

  Future<void> _flush(PushxSnapshot s) async {
    for (var batch = 0; batch < 4; batch++) {
      if (_inactive || !await store.isCurrent(s)) return;
      final acks = await store.acks(s);
      if (acks.isEmpty) return;
      await _guard(_api!.acknowledge(acks.map((a) => a.ack).toList()));
      if (_inactive) return;
      await store.confirmAcks(s, acks);
    }
  }

  Future<void> _status(PushxSnapshot s) async {
    if (_inactive) return;
    _permission = await _guard(presenter.permission());
    final status = <String, dynamic>{
      ..._storeStatus(),
      'lastReportedAt': _clock(),
      'serverOffset': _offset,
      'errorCode': _errors.values.firstOrNull ?? '',
      'errors': Map<String, String>.of(_errors),
      'queued': _queued,
    };
    if (_inactive) return;
    await store.writeStatus(s, status);
    onStatus?.call(status);
  }

  void _interrupt() {
    _timer?.cancel();
    final aborters = List<void Function()>.of(_aborters.values);
    _aborters.clear();
    for (final cancel in aborters) {
      cancel();
    }
    try {
      _api?.close();
    } on Object {
      _errors['cleanup'] = 'api_close_failed';
    }
  }

  void _notifyStopped() {
    if (_notified) return;
    _notified = true;
    _timer?.cancel();
    if (onStopped != null) {
      unawaited(
        Future<void>(() async {
          final current = await store.snapshot();
          if (current != null &&
              !current.enabled &&
              current.runtimeOwner == _owner) {
            await onStopped!();
          }
        }).catchError((Object _) {}),
      );
    }
  }

  /// A superseded engine closes its own I/O only. It never touches binding
  /// credentials, notification cleanup or the process-wide native service.
  Future<void> _retire() async {
    _closed = true;
    _interrupt();
    await _closeTransport();
  }

  /// Bypasses queued work; local disable/recovery intent precedes side effects.
  Future<PushxStopResult> stopBinding({bool revoked = false}) {
    if (_stopFuture != null) return _stopFuture!;
    _stopping = true;
    _interrupt();
    return _stopFuture = (() async {
      try {
        final current = await store.snapshot();
        final owned = _snapshot;
        if (current == null ||
            owned == null ||
            current.runtimeOwner != _owner ||
            owned.runtimeOwner != _owner ||
            current.binding.scope != owned.binding.scope ||
            current.binding.bindingId != owned.binding.bindingId ||
            current.binding.credentialKey != owned.binding.credentialKey) {
          return const PushxStopResult(false);
        }
        return await PushxLifecycle(
          store: store,
          credentials: credentials,
          presenter: presenter,
          apiFactory: _apiFactory,
        ).stop(current, revoked: revoked);
      } finally {
        await _closeTransport();
        _notifyStopped();
      }
    })();
  }

  Future<void> _closeTransport() async {
    final subscriptions = List<StreamSubscription<dynamic>>.of(_subscriptions);
    _subscriptions.clear();
    final transport = _transport, api = _api;
    _transport = null;
    _api = null;
    try {
      api?.close();
    } on Object {
      _errors['cleanup'] = 'api_close_failed';
    }
    for (final sub in subscriptions) {
      try {
        await sub.cancel();
      } on Object {
        _errors['cleanup'] = 'subscription_close_failed';
      }
    }
    try {
      await transport?.close().timeout(const Duration(seconds: 3));
    } on Object {
      _errors['cleanup'] = 'transport_close_failed';
    }
  }

  Future<void> close() {
    if (_closeFuture != null) return _closeFuture!;
    _closed = true;
    _interrupt();
    return _closeFuture = (() async {
      await _tail;
      if (_stopFuture != null) await _stopFuture;
      await _closeTransport();
    })();
  }

  Future<void> get idle => _tail;
}
