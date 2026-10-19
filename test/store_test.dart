import 'dart:convert';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:pushx_flutter/pushx_flutter.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

import 'support.dart';

void main() {
  late Directory temp;
  late PushxStore store;
  late PushxSnapshot active;
  setUpAll(sqfliteFfiInit);
  setUp(() async {
    temp = await Directory.systemTemp.createTemp('pushx-store-');
    store = await PushxStore.open(
      databasePath: '${temp.path}/test.db',
      factory: databaseFactoryFfi,
    );
    await store.activate(config(), binding());
    active = await store.setEnabled(true);
  });
  tearDown(() async {
    await store.close();
    await temp.delete(recursive: true);
  });

  Future<void> removeV5Schema(Database db) async {
    await db.execute('DROP TABLE native_notifications');
    await db.execute('ALTER TABLE runtime DROP COLUMN runtime_owner');
    for (final column in [
      'remote_revoked',
      'service_stopped',
      'credential_deleted',
    ]) {
      await db.execute('ALTER TABLE pending_cleanup DROP COLUMN $column');
    }
  }

  test(
    'ACK persistence and late HTTP completion preserve a newer stage',
    () async {
      await store.accept(active, packet(), testNow);
      final received = await store.acks(active);
      final item = (await store.pending(active, testNow)).single;
      expect(await store.claim(active, item, 'worker', testNow), isTrue);
      expect(await store.finish(active, item, 'worker'), isTrue);
      await store.confirmAcks(active, received);
      expect((await store.acks(active)).single.ack.stage, PushxAckStage.posted);
      await store.close();
      store = await PushxStore.open(
        databasePath: '${temp.path}/test.db',
        factory: databaseFactoryFfi,
      );
      expect((await store.acks(active)).single.ack.stage, PushxAckStage.posted);
      await store.accept(active, packet(), testNow);
      expect(await store.pending(active, testNow), isEmpty);
    },
  );

  test('two connections cannot both claim the same presentation', () async {
    await store.accept(active, packet(), testNow);
    final item = (await store.pending(active, testNow)).single;
    final second = await PushxStore.open(
      databasePath: '${temp.path}/test.db',
      factory: databaseFactoryFfi,
    );
    try {
      final claims = await Future.wait([
        store.claim(active, item, 'one', testNow),
        second.claim(active, item, 'two', testNow),
      ]);
      expect(claims.where((v) => v).length, 1);
      expect(
        await store.claim(active, item, 'recovery', testNow + 30001),
        isTrue,
      );
      expect(
        await store.finish(active, item, claims.first ? 'one' : 'two'),
        isFalse,
      );
      expect(await store.finish(active, item, 'recovery'), isTrue);
    } finally {
      await second.close();
    }
  });

  test(
    'same event with another delivery ID uses one notification ID',
    () async {
      await store.accept(active, packet(), testNow);
      await store.accept(active, packet(id: 'delivery-2'), testNow);
      final pending = await store.pending(active, testNow);
      expect(pending.length, 1);
      expect((await store.notificationIds(active)).length, 1);
      await store.claim(active, pending.single, 'worker', testNow);
      await store.finish(active, pending.single, 'worker');
      expect(
        (await store.acks(active)).map((a) => a.ack.stage),
        everyElement(PushxAckStage.posted),
      );
    },
  );

  test(
    'old revision cannot display or complete after identity replacement',
    () async {
      await store.accept(active, packet(), testNow);
      final item = (await store.pending(active, testNow)).single;
      await store.claim(active, item, 'worker', testNow);
      await store.activate(config(), binding(id: 'new-binding'));
      final next = await store.setEnabled(true);
      expect(await store.finish(active, item, 'worker'), isFalse);
      expect(await store.disable(active), isFalse);
      expect(await store.isCurrent(next), isTrue);
      expect(await store.accept(next, packet(), testNow), isFalse);
    },
  );

  test('click survives reopening and is discarded on stop', () async {
    await store.accept(active, packet(), testNow);
    final item = (await store.pending(active, testNow)).single;
    await store.claim(active, item, 'worker', testNow);
    await store.finish(active, item, 'worker');
    await store.recordOpen(
      jsonEncode({
        'scope': active.binding.scope,
        'bindingId': 'binding-1',
        'notificationId': 'delivery-1',
      }),
      testNow,
    );
    await store.close();
    store = await PushxStore.open(
      databasePath: '${temp.path}/test.db',
      factory: databaseFactoryFfi,
    );
    expect(
      (await store.readPendingOpens(testNow)).single.packet.notificationId,
      'delivery-1',
    );
    await store.disable(active);
    expect(await store.readPendingOpens(testNow), isEmpty);
  });

  test('expired packet is terminally blocked without presentation', () async {
    await store.accept(active, packet(expires: testNow), testNow);
    expect(await store.pending(active, testNow), isEmpty);
    final ack = (await store.acks(active)).single.ack;
    expect(ack.stage, PushxAckStage.blocked);
    expect(ack.reason, 'delivery_expired');
  });

  test(
    'stop commits recovery intent before process death and protects live identity purge',
    () async {
      await store.accept(active, packet(), testNow);
      expect(await store.purgeBinding(active), isFalse);
      expect(await store.beginStop(active), isTrue);
      await store.close();
      store = await PushxStore.open(
        databasePath: '${temp.path}/test.db',
        factory: databaseFactoryFfi,
      );
      expect((await store.snapshot())!.enabled, isFalse);
      expect(await store.readPendingCleanup(), hasLength(1));
      expect(await store.readPendingRevocations(), hasLength(1));
      expect(await store.purgeBinding(active), isFalse);
      await store.prune(testNow + const Duration(days: 8).inMilliseconds);
      expect(await store.notificationIds(active), hasLength(1));
    },
  );

  test(
    'retired binding cannot exhaust the current binding inbox quota',
    () async {
      final limited = PushxConfig.fromJson({
        ...config().toJson(),
        'maxInboxItems': 1,
      });
      await store.activate(limited, binding());
      active = await store.setEnabled(true);
      await store.accept(active, packet(), testNow);
      await store.disable(active);
      await store.activate(limited, binding(id: 'new-binding'));
      final next = await store.setEnabled(true);
      expect(
        await store.accept(next, packet(binding: 'new-binding'), testNow),
        isTrue,
      );
      await expectLater(
        store.accept(
          next,
          packet(id: 'two', event: 'two', binding: 'new-binding'),
          testNow,
        ),
        throwsStateError,
      );
      expect(await store.purgeBinding(active), isTrue);
    },
  );

  test(
    'v2 upgrade preserves data and creates crash recovery storage',
    () async {
      await store.accept(active, packet(), testNow);
      await store.close();
      final old = await databaseFactoryFfi.openDatabase('${temp.path}/test.db');
      await removeV5Schema(old);
      await old.execute('DROP TABLE pending_cleanup');
      await old.execute('ALTER TABLE runtime DROP COLUMN stop_result');
      await old.setVersion(2);
      await old.close();
      store = await PushxStore.open(
        databasePath: '${temp.path}/test.db',
        factory: databaseFactoryFfi,
      );
      expect(await store.isCurrent(active), isTrue);
      expect(await store.beginStop(active), isTrue);
      expect(await store.readPendingCleanup(), hasLength(1));
    },
  );

  test(
    'v3 upgrade retains interrupted cleanup and adds stop result storage',
    () async {
      await store.beginStop(active);
      await store.close();
      final old = await databaseFactoryFfi.openDatabase('${temp.path}/test.db');
      await removeV5Schema(old);
      await old.execute('ALTER TABLE runtime DROP COLUMN stop_result');
      await old.setVersion(3);
      await old.close();
      store = await PushxStore.open(
        databasePath: '${temp.path}/test.db',
        factory: databaseFactoryFfi,
      );
      expect(await store.readPendingCleanup(), hasLength(1));
      expect(await store.completedStop((await store.snapshot())!), isNull);
      await store.completeCleanup(active, remoteRevoked: true);
      expect(await store.completedStop((await store.snapshot())!), isTrue);
    },
  );

  test(
    'completed click cannot be queued again by a retained launch intent',
    () async {
      await store.accept(active, packet(), testNow);
      final item = (await store.pending(active, testNow)).single;
      await store.claim(active, item, 'worker', testNow);
      await store.finish(active, item, 'worker');
      final payload = jsonEncode({
        'scope': active.binding.scope,
        'bindingId': active.binding.bindingId,
        'notificationId': item.packet.notificationId,
      });
      await store.recordOpen(payload, testNow);
      final click = (await store.readPendingOpens(testNow)).single;
      await store.completeOpen(click.id);
      await store.close();
      store = await PushxStore.open(
        databasePath: '${temp.path}/test.db',
        factory: databaseFactoryFfi,
      );
      await store.recordOpen(payload, testNow);
      expect(await store.readPendingOpens(testNow), isEmpty);
    },
  );

  test(
    'v1 upgrade preserves the active binding, inbox and pending ACK',
    () async {
      await store.accept(active, packet(), testNow);
      await store.close();
      final old = await databaseFactoryFfi.openDatabase('${temp.path}/test.db');
      await removeV5Schema(old);
      await old.execute('ALTER TABLE inbox DROP COLUMN opened');
      await old.execute('ALTER TABLE runtime DROP COLUMN stop_result');
      await old.setVersion(1);
      await old.close();
      store = await PushxStore.open(
        databasePath: '${temp.path}/test.db',
        factory: databaseFactoryFfi,
      );
      expect(await store.isCurrent(active), isTrue);
      final item = (await store.pending(active, testNow)).single;
      expect(item.packet.notificationId, 'delivery-1');
      expect(
        (await store.acks(active)).single.ack.stage,
        PushxAckStage.received,
      );
      await store.claim(active, item, 'worker', testNow);
      expect(await store.finish(active, item, 'worker'), isTrue);
    },
  );
  test(
    'runtime ownership is fenced across database connections and preference updates',
    () async {
      final second = await PushxStore.open(
        databasePath: '${temp.path}/test.db',
        factory: databaseFactoryFfi,
      );
      try {
        final first = await store.claimRuntime('engine-one');
        final newer = await second.claimRuntime('engine-two');
        await second.updateConfig(
          PushxConfig.fromJson({...config().toJson(), 'sound': false}),
        );
        final current = (await store.snapshot())!;
        expect(current.runtimeOwner, 'engine-two');
        expect(current.revision, greaterThan(newer.revision));
        expect(await store.beginStop(first, revoked: true), isFalse);
        expect(await store.isCurrent(first), isFalse);
        expect(await store.isCurrent(current), isTrue);
        expect(await store.readPendingCleanup(), isEmpty);
      } finally {
        await second.close();
      }
    },
  );

  test(
    'v4 migration retains posted/presenting native IDs and prior revoke proof',
    () async {
      await store.accept(active, packet(), testNow);
      final posted = (await store.pending(active, testNow)).single;
      await store.claim(active, posted, 'owner', testNow);
      await store.finish(active, posted, 'owner');
      await store.accept(active, packet(id: 'crash', event: 'crash'), testNow);
      final presenting = (await store.pending(active, testNow)).single;
      await store.claim(active, presenting, 'crashed-engine', testNow);
      await store.beginStop(active, revoked: true);
      await store.close();
      final old = await databaseFactoryFfi.openDatabase('${temp.path}/test.db');
      await removeV5Schema(old);
      await old.setVersion(4);
      await old.close();
      store = await PushxStore.open(
        databasePath: '${temp.path}/test.db',
        factory: databaseFactoryFfi,
      );
      expect((await store.cleanupProgress(active))!.remoteRevoked, isTrue);
      final cleanup = (await store.readPendingCleanup()).single;
      await store.prune(testNow + const Duration(days: 8).inMilliseconds);
      expect(
        await store.notificationIds(active),
        unorderedEquals([posted.systemId, presenting.systemId]),
      );
      // Successful cancellation survives process death while the other ID remains.
      await store.confirmNotificationCanceled(cleanup, posted.systemId);
      await store.close();
      store = await PushxStore.open(
        databasePath: '${temp.path}/test.db',
        factory: databaseFactoryFfi,
      );
      expect(await store.notificationIds(active), [presenting.systemId]);
      expect((await store.cleanupProgress(active))!.remoteRevoked, isTrue);
    },
  );

  test(
    'native registry reserves IDs across retention and protects retired binding purge',
    () async {
      await store.accept(active, packet(), testNow);
      final item = (await store.pending(active, testNow)).single;
      await store.claim(active, item, 'owner', testNow);
      await store.finish(active, item, 'owner');
      await store.prune(testNow + const Duration(days: 8).inMilliseconds);
      await store.accept(
        active,
        packet(
          id: 'later',
          expires: testNow + const Duration(days: 10).inMilliseconds,
        ),
        testNow,
      );
      expect(
        (await store.pending(active, testNow)).single.systemId,
        item.systemId,
      );
      await store.disable(active);
      expect(await store.purgeBinding(active), isFalse);
    },
  );
}
