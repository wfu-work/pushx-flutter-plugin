import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:pushx_flutter/pushx_flutter.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

import 'support.dart';

void main() {
  late Directory temp;
  late PushxStore store;
  late PushxRuntime runtime;
  late FakeTransport transport;
  late FakePresenter presenter;
  late FakeApi api;
  late MemoryCredentials credentials;
  var now = testNow;
  setUpAll(sqfliteFfiInit);
  setUp(() async {
    now = testNow;
    temp = await Directory.systemTemp.createTemp('pushx-runtime-');
    store = await PushxStore.open(
      databasePath: '${temp.path}/test.db',
      factory: databaseFactoryFfi,
    );
    await store.activate(config(), binding());
    await store.setEnabled(true);
    transport = FakeTransport();
    presenter = FakePresenter();
    api = FakeApi();
    credentials = MemoryCredentials();
    runtime = PushxRuntime(
      store: store,
      credentials: credentials,
      presenter: presenter,
      apiFactory: (_, _) => api,
      transportFactory: (_, _) => transport,
      clock: () => now,
      jitter: () => 0.5,
    );
  });
  tearDown(() async {
    await runtime.close();
    await transport.dispose();
    await store.close();
    await temp.delete(recursive: true);
  });

  test(
    'duplicate push displays once and ACK survives a network failure',
    () async {
      await runtime.start(periodic: false);
      await settle(runtime);
      api.failAck = true;
      transport.send(packet());
      await settle(runtime);
      transport.send(packet());
      await settle(runtime);
      expect(presenter.shown.length, 1);
      expect(
        (await store.acks((await store.snapshot())!)).single.ack.stage,
        PushxAckStage.posted,
      );
      api.failAck = false;
      now += 15001;
      api.now = now;
      await runtime.tick();
      expect(await store.acks((await store.snapshot())!), isEmpty);
      expect(api.acknowledged.last.stage, PushxAckStage.posted);
    },
  );

  test(
    'display crash recovers the same system ID silently after its lease',
    () async {
      var fail = true;
      presenter.beforeShow = () async {
        if (fail) {
          fail = false;
          throw StateError('simulated crash after submit');
        }
      };
      await runtime.start(periodic: false);
      await settle(runtime);
      transport.send(packet());
      await settle(runtime);
      now += 31000;
      api.now = now;
      await runtime.tick();
      expect(presenter.shown.length, 2);
      expect(presenter.shown.toSet().length, 1);
      expect(presenter.silenced.last, isTrue);
      expect(api.acknowledged.last.stage, PushxAckStage.posted);
    },
  );

  test(
    'identity invalidation during native show cancels the late notification',
    () async {
      final entered = Completer<void>(), release = Completer<void>();
      presenter.beforeShow = () async {
        entered.complete();
        await release.future;
      };
      await runtime.start(periodic: false);
      await settle(runtime);
      transport.send(packet());
      await entered.future;
      final old = (await store.snapshot())!;
      await store.disable(old);
      await store.activate(config(), binding(id: 'new-binding'));
      final next = await store.setEnabled(true);
      release.complete();
      await settle(runtime);
      expect(presenter.canceled, contains(presenter.shown.single));
      expect(await store.isCurrent(next), isTrue);
      expect(
        api.acknowledged.where((a) => a.stage == PushxAckStage.posted),
        isEmpty,
      );
    },
  );

  test('unsupported protocol is rejected durably without showing', () async {
    await runtime.start(periodic: false);
    await settle(runtime);
    transport.publications.add(
      utf8.encode(jsonEncode({...packet().toJson(), 'protocolVersion': 2})),
    );
    await settle(runtime);
    expect(presenter.shown, isEmpty);
    expect(api.acknowledged.last.reason, 'unsupported_version');
  });

  test(
    'HTTP revocation disables locally and deletes the notification credential',
    () async {
      api.deny = true;
      await runtime.start(periodic: false);
      await settle(runtime);
      expect((await store.snapshot())!.enabled, isFalse);
      expect(credentials.values, isEmpty);
      expect(transport.closed, isTrue);
    },
  );

  test('each synchronization round restarts temporary pagination', () async {
    api.onSync = (cursor) async => cursor.isEmpty
        ? PushxSyncPage([packet().toJson()], '10', true, now)
        : PushxSyncPage(
            [packet(id: 'delivery-2', event: 'event:2').toJson()],
            '',
            false,
            now,
          );
    await runtime.start(periodic: false);
    await settle(runtime);
    expect(api.cursors.take(2), ['', '10']);
    now += 300001;
    api.now = now;
    final before = api.cursors.length;
    await runtime.tick();
    expect(api.cursors[before], '');
    expect(presenter.shown.length, 2);
  });

  test(
    'sync and ACK failures cannot starve renewal across the original expiry',
    () async {
      await runtime.start(periodic: false);
      await settle(runtime);
      now += 7 * 86400000 - 3600000;
      api.now = now;
      api.onSync = (_) async => throw const PushxApiException('http_failure');
      api.failAck = true;
      await store.accept(
        (await store.snapshot())!,
        packet(expires: now + 86400000),
        now,
      );
      await runtime.tick();
      expect(api.renewCalls, 1);
      expect((await store.snapshot())!.binding.expiresAt, now + 7 * 86400000);
      now += 3600001;
      api.now = now;
      await runtime.tick();
      expect((await store.snapshot())!.enabled, isTrue);
      expect(api.ackCalls, greaterThan(0));
    },
  );

  test(
    'ticks coalesce and stop interrupts a stalled request without its cooperation',
    () async {
      await runtime.start(periodic: false);
      await settle(runtime);
      final entered = Completer<void>(), release = Completer<PushxSyncPage>();
      api.onSync = (_) {
        entered.complete();
        return release.future;
      };
      now += 300001;
      api.now = now;
      final busy = runtime.tick();
      await entered.future;
      for (var i = 0; i < 100; i++) {
        expect(identical(runtime.tick(), busy), isTrue);
      }
      final result = await runtime.stopBinding().timeout(
        const Duration(seconds: 1),
      );
      await busy.timeout(const Duration(seconds: 1));
      expect(result.remoteRevoked, isTrue);
      expect((await store.snapshot())!.enabled, isFalse);
      expect(credentials.values, isEmpty);
      expect(transport.closed, isTrue);
      release.complete(PushxSyncPage([packet().toJson()], '', false, now));
      await settle(runtime);
      expect(presenter.shown, isEmpty);
    },
  );

  test(
    'close aborts stalled I/O and is idempotent without disabling the binding',
    () async {
      await runtime.start(periodic: false);
      await settle(runtime);
      final entered = Completer<void>(), release = Completer<PushxSyncPage>();
      api.onSync = (_) {
        entered.complete();
        return release.future;
      };
      now += 300001;
      final busy = runtime.tick();
      await entered.future;
      final closed = runtime.close();
      expect(identical(runtime.close(), closed), isTrue);
      await closed.timeout(const Duration(seconds: 1));
      await busy;
      expect((await store.snapshot())!.enabled, isTrue);
      release.complete(PushxSyncPage([], '', false, now));
    },
  );

  test('renew retries back off while sync continues to receive', () async {
    await runtime.start(periodic: false);
    await settle(runtime);
    now += 7 * 86400000 - 3600000;
    api.now = now;
    api.onRenew = () async =>
        throw const PushxApiException('network_unavailable');
    api.onSync = (_) async =>
        PushxSyncPage([packet(expires: now + 60000).toJson()], '', false, now);
    await runtime.tick();
    expect(api.renewCalls, 1);
    expect(presenter.shown, hasLength(1));
    await runtime.tick();
    expect(api.renewCalls, 1);
    now += 15001;
    api.now = now;
    await runtime.tick();
    expect(api.renewCalls, 2);
  });

  test('connection events cannot spin when the packet queue is full', () async {
    await runtime.start(periodic: false);
    await settle(runtime);
    final entered = Completer<void>(), release = Completer<void>();
    api.onAcknowledge = (_) {
      if (!entered.isCompleted) entered.complete();
      return release.future;
    };
    final data = utf8.encode(jsonEncode(packet().toJson()));
    transport.publications.add(data);
    await entered.future;
    for (var i = 0; i < 250; i++) {
      transport.publications.add(data);
    }
    transport.connections.add(
      const PushxConnectionEvent('connected', serverNow: testNow),
    );
    await Future<void>.delayed(const Duration(milliseconds: 10));
    await runtime.close().timeout(const Duration(seconds: 1));
    release.complete();
  });
  test(
    'superseded runtime cannot adopt preferences or stop the newer owner',
    () async {
      var oldNativeStops = 0;
      runtime = PushxRuntime(
        store: store,
        credentials: credentials,
        presenter: presenter,
        apiFactory: (_, _) => api,
        transportFactory: (_, _) => transport,
        clock: () => now,
        onStopped: () {
          oldNativeStops++;
        },
      );
      await runtime.start(periodic: false);
      await settle(runtime);
      final nextTransport = FakeTransport();
      final newer = PushxRuntime(
        store: store,
        credentials: credentials,
        presenter: presenter,
        apiFactory: (_, _) => FakeApi(),
        transportFactory: (_, _) => nextTransport,
        clock: () => now,
      );
      try {
        await store.setEnabled(true);
        await newer.start(periodic: false);
        await settle(newer);
        final owner = (await store.snapshot())!.runtimeOwner;
        await store.updateConfig(
          PushxConfig.fromJson({...config().toJson(), 'sound': false}),
        );
        await runtime
            .tick(); // Must retire, not adopt the newer revision/owner.
        transport.connections.add(
          const PushxConnectionEvent('disconnected', denied: true),
        );
        await runtime.stopBinding(revoked: true); // Also guards late HTTP 401.
        await runtime.close();
        await Future<void>.delayed(Duration.zero);
        expect((await store.snapshot())!.enabled, isTrue);
        expect((await store.snapshot())!.runtimeOwner, owner);
        expect(credentials.values['test-key'], testCredential);
        expect(await store.readPendingCleanup(), isEmpty);
        expect(oldNativeStops, 0);
        await newer.tick();
        nextTransport.send(packet());
        await settle(newer);
        expect(presenter.shown, hasLength(1));
      } finally {
        await newer.close();
        await nextTransport.dispose();
      }
    },
  );

  test(
    '3501 closes only the replaced connection; genuine denial still stops current owner',
    () async {
      expect(
        const PushxConnectionEvent(
          'disconnected',
          disconnectCode: 3501,
        ).superseded,
        isTrue,
      );
      expect(
        const PushxConnectionEvent('disconnected', disconnectCode: 3501).denied,
        isFalse,
      );
      expect(
        const PushxConnectionEvent('disconnected', disconnectCode: 3005).denied,
        isFalse,
      );
      await runtime.start(periodic: false);
      await settle(runtime);
      final nextTransport = FakeTransport();
      final newer = PushxRuntime(
        store: store,
        credentials: credentials,
        presenter: presenter,
        apiFactory: (_, _) => FakeApi(),
        transportFactory: (_, _) => nextTransport,
        clock: () => now,
      );
      try {
        await newer.start(
          periodic: false,
        ); // Same binding, no setEnabled/revision trick.
        await settle(newer);
        transport.connections.add(
          const PushxConnectionEvent('disconnected', disconnectCode: 3501),
        );
        await settle(runtime);
        expect(transport.closed, isTrue);
        expect((await store.snapshot())!.enabled, isTrue);
        expect(credentials.values, isNotEmpty);
        nextTransport.connections.add(
          const PushxConnectionEvent('disconnected', disconnectCode: 3500),
        );
        await newer.close();
        expect((await store.snapshot())!.enabled, isFalse);
        expect(credentials.values, isEmpty);
      } finally {
        await newer.close();
        await nextTransport.dispose();
      }
    },
  );

  test(
    'preferences change retains ownership and current-owner denial revokes',
    () async {
      await runtime.start(periodic: false);
      await settle(runtime);
      final owner = (await store.snapshot())!.runtimeOwner;
      await store.updateConfig(
        PushxConfig.fromJson({...config().toJson(), 'sound': false}),
      );
      await runtime.tick();
      await settle(runtime);
      expect((await store.snapshot())!.runtimeOwner, owner);
      transport.connections.add(
        const PushxConnectionEvent('disconnected', denied: true),
      );
      await runtime.close();
      expect((await store.snapshot())!.enabled, isFalse);
      expect(credentials.values, isEmpty);
    },
  );
  test(
    'late authorization denial from old live engine cannot clean a newer engine',
    () async {
      await runtime.start(periodic: false);
      await settle(runtime);
      final nextTransport = FakeTransport();
      final newer = PushxRuntime(
        store: store,
        credentials: credentials,
        presenter: presenter,
        apiFactory: (_, _) => FakeApi(),
        transportFactory: (_, _) => nextTransport,
        clock: () => now,
      );
      try {
        await newer.start(periodic: false);
        await settle(newer);
        nextTransport.send(packet());
        await settle(newer);
        transport.connections.add(
          const PushxConnectionEvent('disconnected', denied: true),
        );
        await runtime.close();
        expect((await store.snapshot())!.enabled, isTrue);
        expect(credentials.values['test-key'], testCredential);
        expect(presenter.canceled, isEmpty);
        expect(await store.readPendingCleanup(), isEmpty);
      } finally {
        await newer.close();
        await nextTransport.dispose();
      }
    },
  );
  test(
    'late show completion from an old engine cannot cancel newer same-binding notification',
    () async {
      final entered = Completer<void>(), release = Completer<void>();
      presenter.beforeShow = () async {
        entered.complete();
        await release.future;
      };
      await runtime.start(periodic: false);
      await settle(runtime);
      transport.send(packet());
      await entered.future;
      final nextTransport = FakeTransport(), nextPresenter = FakePresenter();
      final nextApi = FakeApi()..now = testNow + 31001;
      final newer = PushxRuntime(
        store: store,
        credentials: credentials,
        presenter: nextPresenter,
        apiFactory: (_, _) => nextApi,
        transportFactory: (_, _) => nextTransport,
        clock: () => testNow + 31001,
      );
      try {
        await newer.start(periodic: false);
        await settle(newer);
        expect(nextPresenter.shown, [presenter.shown.single]);
        release.complete();
        await settle(runtime);
        expect(presenter.canceled, isEmpty);
        expect((await store.snapshot())!.enabled, isTrue);
      } finally {
        if (!release.isCompleted) release.complete();
        await newer.close();
        await nextTransport.dispose();
      }
    },
  );
  test(
    'late native show after stop and database disposal still cancels retired notification',
    () async {
      final entered = Completer<void>(),
          release = Completer<void>(),
          canceled = Completer<void>();
      presenter.beforeShow = () async {
        entered.complete();
        await release.future;
      };
      presenter.beforeCancel = () async {
        if (release.isCompleted && !canceled.isCompleted) canceled.complete();
      };
      await runtime.start(periodic: false);
      await settle(runtime);
      transport.send(packet());
      await entered.future;
      await runtime.stopBinding();
      await runtime.close();
      await store.close();
      release.complete();
      await canceled.future.timeout(const Duration(seconds: 2));
      store = await PushxStore.open(
        databasePath: '${temp.path}/test.db',
        factory: databaseFactoryFfi,
      );
      expect((await store.snapshot())!.enabled, isFalse);
      expect(presenter.canceled, contains(presenter.shown.single));
    },
  );
  test(
    'preferences changed during installation reload without retiring the owner',
    () async {
      final entered = Completer<void>(), release = Completer<void>();
      presenter.beforeInitialize = () async {
        if (!entered.isCompleted) {
          entered.complete();
          await release.future;
        }
      };
      final starting = runtime.start(periodic: false);
      await entered.future;
      final owner = (await store.snapshot())!.runtimeOwner;
      await store.updateConfig(
        PushxConfig.fromJson({...config().toJson(), 'sound': false}),
      );
      release.complete();
      await starting;
      await settle(runtime);
      transport.send(packet());
      await settle(runtime);
      expect(presenter.shown, hasLength(1));
      expect((await store.snapshot())!.runtimeOwner, owner);
      expect((await store.snapshot())!.enabled, isTrue);
    },
  );
  test(
    'durable owner reconnects if an older late socket replaces it',
    () async {
      final replacement = FakeTransport();
      var connects = 0, nativeStops = 0;
      runtime = PushxRuntime(
        store: store,
        credentials: credentials,
        presenter: presenter,
        apiFactory: (_, _) => api,
        transportFactory: (_, _) => connects++ == 0 ? transport : replacement,
        clock: () => now,
        onStopped: () {
          nativeStops++;
        },
      );
      try {
        await runtime.start(periodic: false);
        await settle(runtime);
        transport.connections.add(
          const PushxConnectionEvent('disconnected', disconnectCode: 3501),
        );
        await settle(runtime);
        replacement.send(packet());
        await settle(runtime);
        expect(connects, 2);
        expect(presenter.shown, hasLength(1));
        expect((await store.snapshot())!.enabled, isTrue);
        expect(credentials.values, isNotEmpty);
        expect(nativeStops, 0);
      } finally {
        await runtime.close();
        await replacement.dispose();
      }
    },
  );
}
