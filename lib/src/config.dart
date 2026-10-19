import 'dart:convert';

import 'protocol.dart';

class PushxEndpoints {
  const PushxEndpoints({
    this.websocket = 'mobile/notifications/ws',
    this.sync = 'mobile/notifications/sync',
    this.ack = 'mobile/notifications/ack',
    this.renew = 'mobile/notifications/renew',
    this.status = 'mobile/notifications/status',
    this.stop = 'mobile/notifications/stop',
  });
  final String websocket, sync, ack, renew, status, stop;
  Json toJson() => {
    'websocket': websocket,
    'sync': sync,
    'ack': ack,
    'renew': renew,
    'status': status,
    'stop': stop,
  };
  factory PushxEndpoints.fromJson(Json j) => PushxEndpoints(
    websocket: jsonString(j, 'websocket'),
    sync: jsonString(j, 'sync'),
    ack: jsonString(j, 'ack'),
    renew: jsonString(j, 'renew'),
    status: jsonString(j, 'status'),
    stop: jsonString(j, 'stop'),
  );
}

class PushxConfig {
  PushxConfig({
    required this.hub,
    this.endpoints = const PushxEndpoints(),
    this.allowInsecure = false,
    this.sound = true,
    this.icon = 'ic_notification',
    this.channelId = 'pushx_events',
    this.channelName = '消息提醒',
    this.serviceTitle = '消息服务运行中',
    this.serviceText = '接收后台消息，点击停止可关闭',
    this.allowWakeLock = false,
    this.maxInboxItems = 5000,
    this.retentionDays = 7,
  }) {
    if (!hub.hasAuthority ||
        hub.host.isEmpty ||
        hub.userInfo.isNotEmpty ||
        hub.hasQuery ||
        hub.hasFragment ||
        (hub.scheme != 'https' && !(allowInsecure && hub.scheme == 'http'))) {
      throw ArgumentError(
        'Pushx requires an HTTPS Hub URL without credentials/query',
      );
    }
    for (final path in endpoints.toJson().values.cast<String>()) {
      endpoint(path);
    }
    if (!RegExp(r'^[a-z][a-z0-9_]{0,63}$').hasMatch(icon) ||
        !RegExp(r'^[A-Za-z0-9_.-]{1,64}$').hasMatch(channelId) ||
        channelName.isEmpty ||
        serviceTitle.isEmpty ||
        utf8.encode(serviceTitle).length > 255 ||
        maxInboxItems < 1 ||
        maxInboxItems > 50000 ||
        retentionDays < 0 ||
        retentionDays > 30) {
      throw ArgumentError('Invalid Pushx notification configuration');
    }
  }

  final Uri hub;
  final PushxEndpoints endpoints;
  final bool allowInsecure, sound, allowWakeLock;
  final int maxInboxItems, retentionDays;
  final String icon, channelId, channelName, serviceTitle, serviceText;

  Uri endpoint(String path, {bool websocket = false}) {
    final p = Uri.parse(path);
    if (p.hasScheme ||
        p.hasAuthority ||
        p.hasQuery ||
        p.hasFragment ||
        p.path.startsWith('/') ||
        p.pathSegments.isEmpty ||
        p.pathSegments.any(
          (s) => s == '..' || s == '.' || s.contains('/') || s.contains('\\'),
        )) {
      throw ArgumentError(
        'Pushx endpoints must be relative paths within the Hub',
      );
    }
    final basePath = hub.path.endsWith('/') ? hub.path : '${hub.path}/';
    return hub.replace(
      scheme: websocket ? (hub.scheme == 'https' ? 'wss' : 'ws') : hub.scheme,
      path: '$basePath${p.path}',
    );
  }

  Json toJson() => {
    'hub': hub.toString(),
    'endpoints': endpoints.toJson(),
    'allowInsecure': allowInsecure,
    'sound': sound,
    'icon': icon,
    'channelId': channelId,
    'channelName': channelName,
    'serviceTitle': serviceTitle,
    'serviceText': serviceText,
    'allowWakeLock': allowWakeLock,
    'maxInboxItems': maxInboxItems,
    'retentionDays': retentionDays,
  };

  factory PushxConfig.fromJson(Json j) => PushxConfig(
    hub: Uri.parse(jsonString(j, 'hub')),
    endpoints: PushxEndpoints.fromJson(jsonObject(j['endpoints'])),
    allowInsecure: j['allowInsecure'] == true,
    sound: j['sound'] == true,
    icon: jsonString(j, 'icon'),
    channelId: jsonString(j, 'channelId'),
    channelName: jsonString(j, 'channelName'),
    serviceTitle: jsonString(j, 'serviceTitle'),
    serviceText: jsonString(j, 'serviceText'),
    allowWakeLock: j['allowWakeLock'] == true,
    maxInboxItems: j['maxInboxItems'] is int ? j['maxInboxItems'] as int : 5000,
    retentionDays: j['retentionDays'] is int ? j['retentionDays'] as int : 7,
  );
}

/// No token is serialized into the SQLite runtime configuration.
class PushxBinding {
  PushxBinding({
    required this.scope,
    required this.subscriptionId,
    required this.bindingId,
    required this.credentialKey,
    required this.expiresAt,
  }) {
    if (scope.isEmpty ||
        scope.length > 512 ||
        !pushxIdentifier(subscriptionId) ||
        !pushxIdentifier(bindingId) ||
        credentialKey.isEmpty ||
        expiresAt <= 0) {
      throw ArgumentError('Invalid Pushx binding');
    }
  }
  final String scope, subscriptionId, bindingId, credentialKey;
  final int expiresAt;
  Json toJson() => {
    'scope': scope,
    'subscriptionId': subscriptionId,
    'bindingId': bindingId,
    'credentialKey': credentialKey,
    'expiresAt': expiresAt,
  };
  factory PushxBinding.fromJson(Json j) => PushxBinding(
    scope: jsonString(j, 'scope'),
    subscriptionId: jsonString(j, 'subscriptionId'),
    bindingId: jsonString(j, 'bindingId'),
    credentialKey: jsonString(j, 'credentialKey'),
    expiresAt: jsonInt(j, 'expiresAt'),
  );
}

class PushxSnapshot {
  const PushxSnapshot(
    this.config,
    this.binding,
    this.revision,
    this.enabled, {
    this.runtimeOwner = '',
  });
  final PushxConfig config;
  final PushxBinding binding;
  final int revision;
  final bool enabled;

  /// Changes only when a service engine claims ownership or start invalidates it.
  /// Preference revisions do not transfer this ownership.
  final String runtimeOwner;
}

class PushxOpen {
  const PushxOpen(this.id, this.scope, this.packet);
  final int id;
  final String scope;
  final PushxPacket packet;
}
