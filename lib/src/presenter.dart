import 'dart:convert';

import 'package:flutter_local_notifications/flutter_local_notifications.dart';

import 'config.dart';
import 'store.dart';

abstract interface class PushxPresenter {
  Future<void> initialize(PushxConfig config);
  Future<String> permission();
  Future<bool> requestPermission();

  /// Returns a whitelisted blocked reason or an empty string after submission.
  Future<String> show(
    PushxSnapshot snapshot,
    PushxInboxItem item, {
    bool silent = false,
  });
  Future<void> cancel(int systemId);
}

class PushxLocalNotifications implements PushxPresenter {
  PushxLocalNotifications(this.store);
  final PushxStore store;
  final _plugin = FlutterLocalNotificationsPlugin();
  String? _icon;

  AndroidFlutterLocalNotificationsPlugin? get _android => _plugin
      .resolvePlatformSpecificImplementation<
        AndroidFlutterLocalNotificationsPlugin
      >();

  @override
  Future<void> initialize(PushxConfig config) async {
    final identity = '${config.icon}:${config.channelId}:${config.channelName}';
    if (_icon == identity) return;
    await _plugin.initialize(
      settings: InitializationSettings(
        android: AndroidInitializationSettings(config.icon),
      ),
      onDidReceiveNotificationResponse: (r) async {
        await store.recordOpen(
          r.payload,
          DateTime.now().millisecondsSinceEpoch,
        );
      },
    );
    for (final sound in [true, false]) {
      await _android?.createNotificationChannel(
        AndroidNotificationChannel(
          sound ? config.channelId : '${config.channelId}_silent',
          sound ? config.channelName : '${config.channelName}（静音）',
          importance: Importance.high,
          playSound: sound,
          enableVibration: sound,
        ),
      );
    }
    _icon = identity;
    final launch = await _plugin.getNotificationAppLaunchDetails();
    if (launch?.didNotificationLaunchApp == true) {
      await store.recordOpen(
        launch?.notificationResponse?.payload,
        DateTime.now().millisecondsSinceEpoch,
      );
    }
  }

  @override
  Future<String> permission() async =>
      await _android?.areNotificationsEnabled() == true ? 'granted' : 'denied';
  @override
  Future<bool> requestPermission() async =>
      await _android?.requestNotificationsPermission() ?? false;

  @override
  Future<String> show(
    PushxSnapshot snapshot,
    PushxInboxItem item, {
    bool silent = false,
  }) async {
    if (await permission() != 'granted') return 'permission_denied';
    final config = snapshot.config;
    final p = item.packet;
    final sound = config.sound && p.presentation.sound && !silent;
    final channelId = sound ? config.channelId : '${config.channelId}_silent';
    final channels = await _android?.getNotificationChannels();
    final channel = channels?.where((c) => c.id == channelId).firstOrNull;
    if (channel?.importance == Importance.none) return 'channel_disabled';
    await _plugin.show(
      id: item.systemId,
      title: p.presentation.title,
      body: p.presentation.body,
      notificationDetails: NotificationDetails(
        android: AndroidNotificationDetails(
          channelId,
          sound ? config.channelName : '${config.channelName}（静音）',
          icon: config.icon,
          importance: Importance.high,
          priority: Priority.high,
          playSound: sound,
          enableVibration: sound,
          onlyAlertOnce: true,
          visibility: NotificationVisibility.private,
        ),
      ),
      payload: jsonEncode({
        'scope': snapshot.binding.scope,
        'bindingId': p.bindingId,
        'notificationId': p.notificationId,
      }),
    );
    return '';
  }

  @override
  Future<void> cancel(int systemId) => _plugin.cancel(id: systemId);
}
