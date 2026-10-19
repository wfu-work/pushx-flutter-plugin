import 'dart:convert';

const pushxProtocolVersion = 1;
const pushxMaxPacketBytes = 32768;
const pushxMaxBatchSize = 50;

typedef Json = Map<String, dynamic>;

bool pushxIdentifier(String value) =>
    RegExp(r'^[A-Za-z0-9_.:-]{1,128}$').hasMatch(value);

class PushxProtocolException implements Exception {
  const PushxProtocolException(this.code);
  final String code;
  @override
  String toString() => 'PushxProtocolException($code)';
}

Json jsonObject(Object? value) {
  if (value is! Map<String, dynamic>) {
    throw const PushxProtocolException('invalid_object');
  }
  return value;
}

String jsonString(Json value, String key) {
  final result = value[key];
  if (result is! String) {
    throw const PushxProtocolException('invalid_field');
  }
  return result;
}

int jsonInt(Json value, String key) {
  final result = value[key];
  if (result is! int || result.abs() > 9007199254740991) {
    throw const PushxProtocolException('invalid_field');
  }
  return result;
}

void _fields(Json value, Set<String> fields) {
  if (value.keys.any((key) => !fields.contains(key))) {
    throw const PushxProtocolException('unknown_field');
  }
}

class PushxPresentation {
  const PushxPresentation({
    required this.title,
    required this.body,
    this.sound = true,
  });
  final String title;
  final String body;
  final bool sound;

  factory PushxPresentation.fromJson(Json json) {
    _fields(json, {'title', 'body', 'sound'});
    if (json['sound'] is! bool) {
      throw const PushxProtocolException('invalid_presentation');
    }
    return PushxPresentation(
      title: jsonString(json, 'title'),
      body: jsonString(json, 'body'),
      sound: json['sound'] as bool,
    );
  }

  Json toJson() => {'title': title, 'body': body, 'sound': sound};
}

class PushxPacket {
  const PushxPacket({
    this.protocolVersion = pushxProtocolVersion,
    required this.kind,
    required this.notificationId,
    required this.subscriptionId,
    required this.bindingId,
    required this.eventKey,
    required this.expiresAt,
    required this.presentation,
    required this.payload,
  });

  final int protocolVersion;
  final String kind, notificationId, subscriptionId, bindingId, eventKey;
  final int expiresAt;
  final PushxPresentation presentation;
  final Json payload;

  factory PushxPacket.decode(List<int> bytes) {
    if (bytes.length > pushxMaxPacketBytes) {
      throw const PushxProtocolException('packet_too_large');
    }
    return PushxPacket.fromJson(jsonObject(jsonDecode(utf8.decode(bytes))));
  }

  factory PushxPacket.fromJson(Json json) {
    _fields(json, {
      'protocolVersion',
      'kind',
      'notificationId',
      'subscriptionId',
      'bindingId',
      'eventKey',
      'expiresAt',
      'presentation',
      'payload',
    });
    final result = PushxPacket(
      protocolVersion: jsonInt(json, 'protocolVersion'),
      kind: jsonString(json, 'kind'),
      notificationId: jsonString(json, 'notificationId'),
      subscriptionId: jsonString(json, 'subscriptionId'),
      bindingId: jsonString(json, 'bindingId'),
      eventKey: jsonString(json, 'eventKey'),
      expiresAt: jsonInt(json, 'expiresAt'),
      presentation: PushxPresentation.fromJson(
        jsonObject(json['presentation']),
      ),
      payload: jsonObject(json['payload']),
    );
    result.validate();
    return result;
  }

  void validate({int? now}) {
    if (protocolVersion != pushxProtocolVersion) {
      throw const PushxProtocolException('unsupported_version');
    }
    if (kind != 'business' && kind != 'test') {
      throw const PushxProtocolException('invalid_kind');
    }
    if (!pushxIdentifier(notificationId) ||
        !pushxIdentifier(subscriptionId) ||
        !pushxIdentifier(bindingId)) {
      throw const PushxProtocolException('invalid_identity');
    }
    if (eventKey.trim().isEmpty ||
        utf8.encode(eventKey).length > 256 ||
        expiresAt <= 0 ||
        expiresAt > 9007199254740991) {
      throw const PushxProtocolException('invalid_metadata');
    }
    if (presentation.title.trim().isEmpty ||
        utf8.encode(presentation.title).length > 255 ||
        utf8.encode(presentation.body).length > 1000 ||
        utf8.encode(jsonEncode(toJson())).length > pushxMaxPacketBytes) {
      throw const PushxProtocolException('invalid_presentation');
    }
    if (now != null && expiresAt <= now) {
      throw const PushxProtocolException('delivery_expired');
    }
  }

  Json toJson() => {
    'protocolVersion': protocolVersion,
    'kind': kind,
    'notificationId': notificationId,
    'subscriptionId': subscriptionId,
    'bindingId': bindingId,
    'eventKey': eventKey,
    'expiresAt': expiresAt,
    'presentation': presentation.toJson(),
    'payload': payload,
  };
}

enum PushxAckStage { received, posted, blocked }

class PushxAck {
  const PushxAck(this.notificationId, this.stage, {this.reason = ''});
  final String notificationId;
  final PushxAckStage stage;
  final String reason;

  void validate() {
    const reasons = {
      'permission_denied',
      'channel_disabled',
      'payload_invalid',
      'unsupported_version',
      'delivery_expired',
    };
    if (!pushxIdentifier(notificationId) ||
        (stage == PushxAckStage.blocked
            ? !reasons.contains(reason)
            : reason.isNotEmpty)) {
      throw const PushxProtocolException('invalid_ack');
    }
  }

  Json toJson() {
    validate();
    return {
      'notificationId': notificationId,
      'stage': stage.name,
      if (reason.isNotEmpty) 'reason': reason,
    };
  }
}

/// Implementations must be pure and reconstructible in a background isolate.
abstract interface class PushxPayloadCodec {
  void validate(PushxPacket packet);
}

/// Applications with a business schema should replace this codec.
class PushxJsonPayloadCodec implements PushxPayloadCodec {
  const PushxJsonPayloadCodec();
  @override
  void validate(PushxPacket packet) {}
}
