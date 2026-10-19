import 'dart:async';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:pushx_flutter/pushx_flutter.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

import 'support.dart';

void main() {
  late Directory temp;
  late PushxStore store;
  late FakePresenter presenter;
  late FakePlatform platform;
  late FakeApi api;
  late MemoryCredentials credentials;
  Pushx? sdk;
  setUpAll(sqfliteFfiInit);
  setUp(() async {
    temp = await Directory.systemTemp.createTemp('pushx-lifecycle-');
    store = await PushxStore.open(
      databasePath: '${temp.path}/test.db',
      factory: databaseFactoryFfi,
    );
    await store.activate(config(), binding());
    await store.setEnabled(true);
    presenter = FakePresenter();
    platform = FakePlatform();
    api = FakeApi();
    credentials = MemoryCredentials();
    sdk = null;
  });
  tearDown(() async {
    await sdk?.dispose();
    await store.close();
    await temp.delete(recursive: true);
  });
  PushxLifecycle lifecycle() => PushxLifecycle(
    store: store,
    credentials: credentials,
    presenter: presenter,
    apiFactory: (_, _) => api,
    stopService: platform.stop,
  );

  test(
    'partial cleanup failure preserves durable intent and retries every component',
    () async {
      final active = (await store.snapshot())!;
      await store.accept(active, packet(), testNow);
      platform.failStop = true;
      credentials.failDelete = true;
      presenter.beforeCancel = () async =>
          throw StateError('cancel unavailable');
      api.onStop = () async =>
          throw const PushxApiException('network_unavailable');
      final result = await lifecycle().stop(active);
      expect(result.remoteRevoked, isFalse);
      expect(result.cleanupComplete, isFalse);
      expect(
        result.errorCodes,
        containsAll([
          'service_stop_failed',
          'remote_revoke_failed',
          'notification_cleanup_failed',
          'credential_delete_failed',
        ]),
      );
      expect((await store.snapshot())!.enabled, isFalse);
      expect(await store.readPendingCleanup(), hasLength(1));
      expect(await store.readPendingRevocations(), hasLength(1));
      expect(api.stopCalls, 1);
      await store.close();
      store = await PushxStore.open(
        databasePath: '${temp.path}/test.db',
        factory: databaseFactoryFfi,
      );
      platform.failStop = false;
      credentials.failDelete = false;
      presenter.beforeCancel = null;
      api.onStop = null;
      await lifecycle().recover();
      expect(await store.readPendingCleanup(), isEmpty);
      expect(await store.readPendingRevocations(), isEmpty);
      expect(credentials.values, isEmpty);
      expect(presenter.canceled, hasLength(1));
      expect(api.stopCalls, 2);
    },
  );

  test(
    'missing or unreadable local credential never proves remote revocation',
    () async {
      credentials.values.clear();
      final result = await lifecycle().stop((await store.snapshot())!);
      expect(result.remoteRevoked, isFalse);
      expect(result.cleanupComplete, isTrue);
      expect(await store.readPendingRevocations(), hasLength(1));
      expect(api.stopCalls, 0);
    },
  );

  test(
    'completed stop remains idempotent after reopening the database',
    () async {
      final result = await lifecycle().stop((await store.snapshot())!);
      expect(result.remoteRevoked, isTrue);
      await store.close();
      store = await PushxStore.open(
        databasePath: '${temp.path}/test.db',
        factory: databaseFactoryFfi,
      );
      final repeated = await lifecycle().stop((await store.snapshot())!);
      expect(repeated.remoteRevoked, isTrue);
      expect(api.stopCalls, 1);
      expect(await store.readPendingRevocations(), isEmpty);
    },
  );

  test('old cleanup recovery cannot stop a newer enabled service', () async {
    final old = (await store.snapshot())!;
    await store.beginStop(old);
    await store.activate(
      config(),
      PushxBinding.fromJson({
        ...binding(id: 'new-binding').toJson(),
        'credentialKey': 'new-key',
      }),
    );
    await store.setEnabled(true);
    await lifecycle().recover();
    expect(platform.stops, 0);
    expect((await store.snapshot())!.enabled, isTrue);
    expect((await store.snapshot())!.binding.bindingId, 'new-binding');
  });

  test(
    'credential read failure still deletes the secret and records reconciliation',
    () async {
      credentials.failRead = true;
      final result = await lifecycle().stop((await store.snapshot())!);
      expect(result.remoteRevoked, isFalse);
      expect(result.errorCodes, contains('credential_read_failed'));
      expect(credentials.values, isEmpty);
      expect(await store.readPendingRevocations(), hasLength(1));
    },
  );

  test(
    'concurrent initialization shares one instance and rejects conflicting configuration',
    () async {
      final entered = Completer<void>(), release = Completer<void>();
      platform.onInitialize = () async {
        entered.complete();
        await release.future;
      };
      PushxApi factory(PushxSnapshot _, String _) => api;
      Future<Pushx> initialize() => Pushx.initialize(
        config: config(),
        store: store,
        presenter: presenter,
        platform: platform,
        credentials: credentials,
        apiFactory: factory,
      );
      final first = initialize();
      await entered.future;
      expect(identical(first, initialize()), isTrue);
      expect(
        () => Pushx.initialize(
          config: PushxConfig(hub: Uri.parse('https://other.test/')),
        ),
        throwsStateError,
      );
      release.complete();
      sdk = await first;
      expect(identical(sdk, await initialize()), isTrue);
      expect(platform.initializations, 1);
    },
  );

  test('invalid activation leaves the working identity untouched', () async {
    sdk = await Pushx.initialize(
      config: config(),
      store: store,
      presenter: presenter,
      platform: platform,
      credentials: credentials,
      apiFactory: (_, _) => api,
    );
    final before = (await store.snapshot())!;
    await expectLater(
      sdk!.activate(
        scope: '',
        subscriptionId: 'subscription-1',
        bindingId: 'new-binding',
        credential: testCredential,
        expiresAt: testNow + 100000,
      ),
      throwsArgumentError,
    );
    expect(await store.isCurrent(before), isTrue);
    expect(credentials.values['test-key'], testCredential);
    expect(platform.stops, 0);
    expect(api.stopCalls, 0);
  });

  test('incomplete old cleanup prevents installing new credentials', () async {
    sdk = await Pushx.initialize(
      config: config(),
      store: store,
      presenter: presenter,
      platform: platform,
      credentials: credentials,
      apiFactory: (_, _) => api,
    );
    platform.failStop = true;
    credentials.failDelete = true;
    await expectLater(
      sdk!.activate(
        scope: 'owner-2',
        subscriptionId: 'sub-2',
        bindingId: 'bind-2',
        credential: testCredential,
        expiresAt: testNow + 100000,
      ),
      throwsStateError,
    );
    expect((await store.snapshot())!.binding.bindingId, 'binding-1');
    expect((await store.snapshot())!.enabled, isFalse);
    expect(await store.readPendingCleanup(), hasLength(1));
    await expectLater(sdk!.start(), throwsStateError);
    expect(platform.starts, 0);
  });

  test(
    'dispose is idempotent and rejects new calls while draining accepted work',
    () async {
      sdk = await Pushx.initialize(
        config: config(),
        store: store,
        presenter: presenter,
        platform: platform,
        credentials: credentials,
        apiFactory: (_, _) => api,
      );
      final entered = Completer<void>(), release = Completer<void>();
      presenter.beforeShow = () async {
        entered.complete();
        await release.future;
      };
      final accepted = sdk!.showLocalTest();
      await entered.future;
      final disposed = sdk!.dispose();
      expect(identical(sdk!.dispose(), disposed), isTrue);
      expect(() => sdk!.snapshot(), throwsStateError);
      release.complete();
      await accepted;
      await disposed;
      expect(platform.disposals, 1);
    },
  );
  test(
    'bounded cleanup resumes remaining IDs after reopening and retains revoke proof',
    () async {
      final active = (await store.snapshot())!;
      for (var i = 0; i < 80; i++) {
        await store.accept(
          active,
          packet(id: 'id-$i', event: 'event-$i'),
          testNow,
        );
      }
      presenter.beforeCancel = () =>
          Future<void>.delayed(const Duration(milliseconds: 500));
      final result = await lifecycle().stop(active);
      expect(result.cleanupComplete, isFalse);
      expect(result.remoteRevoked, isTrue);
      await Future<void>.delayed(const Duration(milliseconds: 700));
      final before = presenter.canceled.toSet();
      expect(before.length, inExclusiveRange(0, 80));
      expect((await store.cleanupProgress(active))!.remoteRevoked, isTrue);
      expect((await store.cleanupProgress(active))!.credentialDeleted, isTrue);
      expect(credentials.values, isEmpty);
      await store.close();
      store = await PushxStore.open(
        databasePath: '${temp.path}/test.db',
        factory: databaseFactoryFfi,
      );
      await lifecycle().recover();
      expect(presenter.canceled.toSet(), hasLength(80));
      expect(presenter.canceled, hasLength(80)); // No repeated prefix.
      expect(platform.stops, 1); // Completed native step is not repeated.
      expect(api.stopCalls, 1); // Token was deleted; proof remains durable.
      expect(await store.readPendingCleanup(), isEmpty);
      expect(await store.completedStop((await store.snapshot())!), isTrue);
    },
  );

  test(
    'posted notification ID survives inbox retention and is canceled on stop',
    () async {
      final active = (await store.snapshot())!;
      await store.accept(active, packet(), testNow);
      final item = (await store.pending(active, testNow)).single;
      await store.claim(active, item, 'owner', testNow);
      await presenter.show(active, item);
      await store.finish(active, item, 'owner');
      await store.prune(testNow + const Duration(days: 8).inMilliseconds);
      expect(await store.pending(active, testNow), isEmpty);
      expect(await store.acks(active), isEmpty);
      expect(await store.notificationIds(active), [item.systemId]);
      await store.close();
      store = await PushxStore.open(
        databasePath: '${temp.path}/test.db',
        factory: databaseFactoryFfi,
      );
      final result = await lifecycle().stop((await store.snapshot())!);
      expect(result.cleanupComplete, isTrue);
      expect(presenter.canceled, [item.systemId]);
      expect(await store.notificationIds(active), isEmpty);
    },
  );

  test(
    'partial cancellation failure commits successful IDs and preserves proof across repeated stop',
    () async {
      final active = (await store.snapshot())!;
      for (var i = 0; i < 12; i++) {
        await store.accept(
          active,
          packet(id: 'id-$i', event: 'event-$i'),
          testNow,
        );
      }
      var calls = 0;
      presenter.beforeCancel = () async {
        if (++calls == 2) throw StateError('one cancellation failed');
      };
      final first = await lifecycle().stop(active);
      expect(first.cleanupComplete, isFalse);
      final remaining = await store.notificationIds(active);
      expect(
        remaining,
        hasLength(5),
      ); // One failed ID and the next unstarted wave.
      presenter.beforeCancel = null;
      final retried = await lifecycle().stop((await store.snapshot())!);
      expect(retried.remoteRevoked, isTrue);
      expect(retried.cleanupComplete, isTrue);
      expect(api.stopCalls, 1);
      expect(presenter.canceled.toSet(), hasLength(12));
      expect(presenter.canceled, hasLength(12));
      expect(await store.readPendingRevocations(), isEmpty);
    },
  );
  test(
    'host reconciliation during cleanup cannot be overwritten by a stale false result',
    () async {
      final active = (await store.snapshot())!;
      await store.accept(active, packet(), testNow);
      api.onStop = () async =>
          throw const PushxApiException('network_unavailable');
      presenter.beforeCancel = () => store.completeRevocation(
        active.binding.scope,
        active.binding.bindingId,
      );
      final result = await lifecycle().stop(active);
      expect(result.cleanupComplete, isTrue);
      expect(result.remoteRevoked, isTrue);
      expect(await store.completedStop((await store.snapshot())!), isTrue);
      expect(await store.readPendingRevocations(), isEmpty);
    },
  );
}
