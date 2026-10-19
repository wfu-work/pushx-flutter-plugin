import 'dart:async';
import 'dart:math';
import 'dart:convert';

import 'package:flutter/foundation.dart';
import 'package:flutter_foreground_task/flutter_foreground_task.dart';

import 'api.dart';
import 'background.dart';
import 'config.dart';
import 'credentials.dart';
import 'presenter.dart';
import 'platform.dart';
import 'lifecycle.dart';
export 'lifecycle.dart' show PushxStopResult;
import 'protocol.dart';
import 'runtime.dart';
import 'store.dart';

/// UI facade. It registers the runtime but only start() launches a service, from
/// a visible Android Activity after a deliberate user action.
class Pushx {
  Pushx._(
    this.config,
    this.store,
    this.credentials,
    this._presenter,
    this._entryPoint,
    this._apiFactory,
    this._platform,
  );
  static bool get supported =>
      !kIsWeb && defaultTargetPlatform == TargetPlatform.android;
  final PushxConfig config;
  final PushxStore store;
  final PushxCredentialStore credentials;
  final PushxPresenter _presenter;
  final void Function() _entryPoint;
  final PushxApiFactory _apiFactory;
  final PushxPlatform _platform;
  late final _lifecycle = PushxLifecycle(
    store: store,
    credentials: credentials,
    presenter: _presenter,
    apiFactory: _apiFactory,
    stopService: _platform.stop,
  );
  static Pushx? _instance;
  static Future<Pushx>? _initializing;
  static List<Object?>? _identity;
  Future<void>? _disposeFuture;
  final _states = StreamController<Json>.broadcast();
  Future<void> _tail = Future.value();
  bool _disposed = false;
  Stream<Json> get status => _states.stream;

  /// Repeated identical initialization shares one instance per UI isolate.
  /// Supplied store/presenter/platform are owned until dispose completes.
  static Future<Pushx> initialize({
    required PushxConfig config,
    void Function()? backgroundEntryPoint,
    PushxCredentialStore? credentials,
    PushxApiFactory? apiFactory,
    String? databasePath,
    PushxStore? store,
    PushxPresenter? presenter,
    PushxPlatform? platform,
  }) {
    final entryPoint = backgroundEntryPoint ?? pushxBackgroundEntryPoint;
    final identity = <Object?>[
      jsonEncode(config.toJson()),
      databasePath,
      entryPoint,
      credentials,
      apiFactory,
      store,
      presenter,
      platform,
    ];
    if (_initializing != null || _instance != null) {
      if (_instance?._disposed == true || !listEquals(_identity, identity)) {
        throw StateError(
          'Dispose the existing Pushx instance before changing initialization',
        );
      }
      return _initializing ?? Future.value(_instance!);
    }
    _identity = identity;
    final future = (() async {
      final adapter = platform ?? PushxAndroidPlatform();
      if (!adapter.supported) throw UnsupportedError('Pushx supports Android');
      final database =
          store ?? await PushxStore.open(databasePath: databasePath);
      final notifications = presenter ?? PushxLocalNotifications(database);
      final sdk = Pushx._(
        config,
        database,
        credentials ?? PushxSecureCredentials(),
        notifications,
        entryPoint,
        apiFactory ?? ((s, t) => PushxHttpApi(s, t)),
        adapter,
      );
      try {
        await notifications.initialize(config);
        await adapter.initialize(
          database.databasePath,
          entryPoint,
          sdk._receiveStatus,
        );
        await sdk._lifecycle.recover();
        _instance = sdk;
        return sdk;
      } on Object {
        adapter.dispose(sdk._receiveStatus);
        await database.close();
        rethrow;
      }
    })();
    _initializing = future;
    unawaited(
      future.then<void>(
        (_) {
          _initializing = null;
        },
        onError: (Object _, StackTrace _) {
          _initializing = null;
          _identity = null;
        },
      ),
    );
    return future;
  }

  void _receiveStatus(Object data) {
    if (!_disposed && data is Map<String, dynamic>) _states.add(data);
  }

  Future<T> _serial<T>(Future<T> Function() work) {
    if (_disposed) throw StateError('Pushx is disposed');
    final result = _tail.then((_) => work());
    _tail = result.then<void>((_) {}, onError: (Object _, StackTrace _) {});
    return result;
  }

  Future<PushxBinding> activate({
    required String scope,
    required String subscriptionId,
    required String bindingId,
    required String credential,
    required int expiresAt,
  }) => _serial(() async {
    if (credential.length < 16 ||
        credential.length > 2048 ||
        credential.codeUnits.any((v) => v < 33 || v > 126)) {
      throw ArgumentError('Invalid notification credential');
    }
    final key =
        'pushx.${List.generate(24, (_) => Random.secure().nextInt(256).toRadixString(16).padLeft(2, '0')).join()}';
    final binding = PushxBinding(
      scope: scope,
      subscriptionId: subscriptionId,
      bindingId: bindingId,
      credentialKey: key,
      expiresAt: expiresAt,
    );
    final old = await store.snapshot();
    if (old != null &&
        old.binding.scope == scope &&
        old.binding.bindingId == bindingId) {
      throw StateError('Re-registration must return a fresh binding ID');
    }
    final stopped = await _stop();
    if (!stopped.cleanupComplete) {
      throw StateError('Previous binding cleanup is incomplete; retry stop');
    }
    await credentials.write(key, credential);
    try {
      await store.activate(config, binding);
    } on Object {
      await credentials.delete(key);
      rethrow;
    }
    return binding;
  });

  Future<bool> requestPermission() => _serial(_presenter.requestPermission);

  Future<void> start() => _serial(() async {
    if (!_platform.visible) {
      throw StateError('Start Pushx from a visible Android Activity');
    }
    final s = await store.snapshot();
    if (s != null &&
        (await store.readPendingCleanup()).any(
          (old) =>
              old.binding.scope == s.binding.scope &&
              old.binding.bindingId == s.binding.bindingId,
        )) {
      throw StateError('Finish stopping the previous binding before starting');
    }
    if (s == null ||
        s.config.hub != config.hub ||
        await credentials.read(s.binding.credentialKey) == null) {
      throw StateError(
        'Register and activate a current notification credential first',
      );
    }
    if (await _presenter.permission() != 'granted') {
      throw StateError('Notification permission is required');
    }
    if (await _platform.running) {
      if (s.enabled) return;
      throw StateError('A previous service is still stopping');
    }
    final active = await store.setEnabled(true);
    try {
      await _platform.start(s.config, _entryPoint);
    } on Object {
      await store.disable(active);
      rethrow;
    }
  });

  Future<PushxStopResult> stop() => _serial(_stop);

  Future<PushxStopResult> _stop() async {
    final s = await store.snapshot();
    if (s == null) return const PushxStopResult(false);
    return _lifecycle.stop(s);
  }

  Future<void> updatePreferences({required bool sound}) => _serial(() async {
    final s = await store.snapshot();
    if (s == null) throw StateError('Activate a binding first');
    await store.updateConfig(
      PushxConfig.fromJson({...s.config.toJson(), 'sound': sound}),
    );
    FlutterForegroundTask.sendDataToTask('pushx.reload');
  });

  Future<Json> snapshot() => _serial(() async {
    final s = await store.snapshot();
    final state = await store.status();
    final actualRunning = await _platform.running;
    return {
      ...state,
      'desiredEnabled': s?.enabled ?? false,
      'actualRunning': actualRunning,
      if (!actualRunning) 'service': 'stopped',
      if (!actualRunning) 'connection': 'disconnected',
    };
  });

  Future<List<PushxOpen>> readPendingOpens() => _serial(() async {
    final state = await store.status();
    final offset = state['serverOffset'] is int
        ? state['serverOffset'] as int
        : 0;
    return store.readPendingOpens(
      DateTime.now().millisecondsSinceEpoch + offset,
    );
  });

  Future<void> completeOpen(int id) => _serial(() => store.completeOpen(id));

  Future<String> showLocalTest() => _serial(() async {
    final now = DateTime.now().millisecondsSinceEpoch;
    final s = PushxSnapshot(
      config,
      PushxBinding(
        scope: 'local-test',
        subscriptionId: 'local-test',
        bindingId: 'local-test',
        credentialKey: 'unused',
        expiresAt: now + 60000,
      ),
      0,
      false,
    );
    return _presenter.show(
      s,
      PushxInboxItem(
        PushxPacket(
          kind: 'test',
          notificationId: 'local-test',
          subscriptionId: 'local-test',
          bindingId: 'local-test',
          eventKey: 'local-test',
          expiresAt: now + 60000,
          presentation: const PushxPresentation(
            title: '本地通知测试',
            body: '通知权限和本地展示可用',
          ),
          payload: const {'type': 'local-test'},
        ),
        4202,
        'received',
      ),
    );
  });

  Future<void> dispose() {
    if (_disposeFuture != null) return _disposeFuture!;
    _disposed = true;
    return _disposeFuture = (() async {
      try {
        await _tail;
        _platform.dispose(_receiveStatus);
        await _states.close();
      } finally {
        try {
          await store.close();
        } finally {
          if (identical(_instance, this)) {
            _instance = null;
            _identity = null;
          }
        }
      }
    })();
  }
}
