import 'dart:async';
import 'dart:ui';

import 'package:flutter/widgets.dart';
import 'package:flutter_foreground_task/flutter_foreground_task.dart';

import 'credentials.dart';
import 'presenter.dart';
import 'protocol.dart';
import 'runtime.dart';
import 'store.dart';

const pushxDatabasePathKey = 'pushx.database.path';

@pragma('vm:entry-point')
void pushxBackgroundEntryPoint() => runPushxBackground();

/// Call from a retained top-level entrypoint to install a custom pure codec or
/// response decoder. Do not pass instances created by the UI isolate.
void runPushxBackground({
  PushxPayloadCodec codec = const PushxJsonPayloadCodec(),
  PushxCredentialStore? credentials,
  PushxApiFactory? apiFactory,
}) {
  WidgetsFlutterBinding.ensureInitialized();
  DartPluginRegistrant.ensureInitialized();
  FlutterForegroundTask.setTaskHandler(
    _PushxTaskHandler(codec, credentials, apiFactory),
  );
}

class _PushxTaskHandler extends TaskHandler {
  _PushxTaskHandler(this.codec, this.credentials, this.apiFactory);
  final PushxPayloadCodec codec;
  final PushxCredentialStore? credentials;
  final PushxApiFactory? apiFactory;
  PushxStore? _store;
  PushxRuntime? _runtime;

  @override
  Future<void> onStart(DateTime timestamp, TaskStarter starter) async {
    try {
      final dbPath = await FlutterForegroundTask.getData<String>(
        key: pushxDatabasePathKey,
      );
      final store = await PushxStore.open(databasePath: dbPath);
      _store = store;
      final runtime = PushxRuntime(
        store: store,
        credentials: credentials ?? PushxSecureCredentials(),
        presenter: PushxLocalNotifications(store),
        codec: codec,
        apiFactory: apiFactory,
        onStatus: FlutterForegroundTask.sendDataToMain,
        onStopped: () => FlutterForegroundTask.stopService().then<void>(
          (_) {},
          onError: (Object _) {
            FlutterForegroundTask.sendDataToMain({
              'errorCode': 'service_stop_failed',
            });
          },
        ),
      );
      _runtime = runtime;
      await runtime.start();
    } on Object {
      try {
        final store = _store;
        final snapshot = await store?.snapshot();
        if (store != null && snapshot != null) {
          await store.writeStatus(snapshot, {
            ...await store.status(),
            'service': 'stopped',
            'connection': 'disconnected',
            'lastReportedAt': DateTime.now().millisecondsSinceEpoch,
            'errorCode': 'background_start_failed',
          });
        }
      } on Object {
        // A failed database open cannot persist a diagnostic. The UI still
        // receives the safe code and can observe actualRunning=false.
      }
      FlutterForegroundTask.sendDataToMain({
        'errorCode': 'background_start_failed',
      });
      await FlutterForegroundTask.stopService();
    }
  }

  @override
  void onRepeatEvent(DateTime timestamp) => unawaited(_runtime?.tick());
  @override
  void onReceiveData(Object data) {
    if (data == 'pushx.reload') unawaited(_runtime?.tick());
  }

  @override
  void onNotificationButtonPressed(String id) {
    if (id == 'pushx.stop') {
      unawaited(
        _runtime?.stopBinding().then<void>(
          (_) {},
          onError: (Object _) {
            FlutterForegroundTask.sendDataToMain({
              'errorCode': 'cleanup_failure',
            });
          },
        ),
      );
    }
  }

  @override
  void onNotificationPressed() => FlutterForegroundTask.launchApp();
  @override
  Future<void> onDestroy(DateTime timestamp, bool isTimeout) async {
    try {
      await _runtime?.close();
    } finally {
      await _store?.close();
    }
  }
}
