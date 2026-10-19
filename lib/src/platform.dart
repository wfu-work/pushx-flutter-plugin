import 'dart:ui';

import 'package:flutter/foundation.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_foreground_task/flutter_foreground_task.dart';

import 'background.dart';
import 'config.dart';

/// Injectable Android service boundary. A custom adapter owns its callbacks.
abstract interface class PushxPlatform {
  bool get supported;
  bool get visible;
  Future<bool> get running;
  Future<void> initialize(
    String databasePath,
    void Function() entryPoint,
    void Function(Object) onData,
  );
  Future<void> start(PushxConfig config, void Function() entryPoint);
  Future<void> stop();
  void dispose(void Function(Object) onData);
}

class PushxAndroidPlatform implements PushxPlatform {
  @override
  bool get supported =>
      !kIsWeb && defaultTargetPlatform == TargetPlatform.android;
  @override
  bool get visible =>
      WidgetsBinding.instance.lifecycleState == AppLifecycleState.resumed;
  @override
  Future<bool> get running => FlutterForegroundTask.isRunningService;
  @override
  Future<void> initialize(
    String databasePath,
    void Function() entryPoint,
    void Function(Object) onData,
  ) async {
    if (PluginUtilities.getCallbackHandle(entryPoint) == null) {
      throw ArgumentError(
        'Use a retained top-level Pushx background entrypoint',
      );
    }
    await FlutterForegroundTask.saveData(
      key: pushxDatabasePathKey,
      value: databasePath,
    );
    FlutterForegroundTask.initCommunicationPort();
    FlutterForegroundTask.addTaskDataCallback(onData);
  }

  @override
  Future<void> start(PushxConfig config, void Function() entryPoint) async {
    FlutterForegroundTask.init(
      androidNotificationOptions: AndroidNotificationOptions(
        channelId: 'pushx_service',
        channelName: '后台消息服务',
        onlyAlertOnce: true,
      ),
      iosNotificationOptions: const IOSNotificationOptions(
        showNotification: false,
      ),
      foregroundTaskOptions: ForegroundTaskOptions(
        eventAction: ForegroundTaskEventAction.nothing(),
        autoRunOnBoot: false,
        autoRunOnMyPackageReplaced: false,
        allowAutoRestart: false,
        allowWakeLock: config.allowWakeLock,
        allowWifiLock: false,
        stopWithTask: false,
      ),
    );
    final result = await FlutterForegroundTask.startService(
      serviceId: 4201,
      serviceTypes: [ForegroundServiceTypes.specialUse],
      notificationTitle: config.serviceTitle,
      notificationText: config.serviceText,
      notificationButtons: [
        const NotificationButton(id: 'pushx.stop', text: '停止'),
      ],
      callback: entryPoint,
    );
    if (result is ServiceRequestFailure) {
      throw StateError('Pushx service start failed');
    }
  }

  @override
  Future<void> stop() async {
    if (await running) {
      final result = await FlutterForegroundTask.stopService();
      if (result is ServiceRequestFailure) {
        throw StateError('Pushx service stop failed');
      }
    }
  }

  @override
  void dispose(void Function(Object) onData) =>
      FlutterForegroundTask.removeTaskDataCallback(onData);
}
