import 'dart:async';

import 'api.dart';
import 'config.dart';
import 'credentials.dart';
import 'presenter.dart';
import 'store.dart';

class PushxStopResult {
  const PushxStopResult(
    this.remoteRevoked, {
    this.cleanupComplete = true,
    this.errorCodes = const [],
  });
  final bool remoteRevoked, cleanupComplete;
  final List<String> errorCodes;
}

/// Shared by the UI and background engine. Recovery records contain no token.
class PushxLifecycle {
  PushxLifecycle({
    required this.store,
    required this.credentials,
    required this.presenter,
    required this.apiFactory,
    this.stopService,
  });
  final PushxStore store;
  final PushxCredentialStore credentials;
  final PushxPresenter presenter;
  final PushxApi Function(PushxSnapshot, String) apiFactory;
  final Future<void> Function()? stopService;

  Future<PushxStopResult> stop(PushxSnapshot s, {bool revoked = false}) async {
    final completed = await store.completedStop(s);
    if (completed != null) return PushxStopResult(completed);
    if (!await store.beginStop(s, revoked: revoked)) {
      return const PushxStopResult(
        false,
        cleanupComplete: false,
        errorCodes: ['binding_changed'],
      );
    }
    return cleanup(s, revoked: revoked);
  }

  Future<PushxStopResult> cleanup(
    PushxSnapshot s, {
    bool revoked = false,
  }) async {
    final errors = <String>[];
    Future<bool> attempt(String code, Future<void> Function() action) async {
      try {
        await action().timeout(const Duration(seconds: 3));
        return true;
      } on Object {
        errors.add(code);
        return false;
      }
    }

    // A caller must use a fresh binding/key when re-enabling. Never clean a
    // live binding, including credentials shared by a newer engine.
    final current = await store.snapshot();
    if (current != null &&
        current.enabled &&
        ((current.binding.scope == s.binding.scope &&
                current.binding.bindingId == s.binding.bindingId) ||
            current.binding.credentialKey == s.binding.credentialKey)) {
      return const PushxStopResult(
        false,
        cleanupComplete: false,
        errorCodes: ['binding_changed'],
      );
    }
    final progress = await store.cleanupProgress(s);
    if (progress == null) {
      return PushxStopResult(await store.completedStop(s) ?? false);
    }
    revoked = revoked || progress.remoteRevoked;
    var local = true;
    if (!progress.serviceStopped) {
      local = await attempt('service_stop_failed', () async {
        if (stopService != null && current?.enabled != true) {
          await stopService!();
        }
        // With no native adapter the runtime stops itself through onStopped;
        // do not record a native success that was never observed here.
        if (stopService != null || current?.enabled == true) {
          await store.completeCleanupStep(s, 'service_stopped');
        }
      });
    }
    String? token;
    if (!revoked && !progress.credentialDeleted) {
      await attempt('credential_read_failed', () async {
        token = await credentials.read(s.binding.credentialKey);
      });
    }
    if (!revoked && token != null) {
      PushxApi? api;
      try {
        api = apiFactory(s, token!);
        await api.stop().timeout(const Duration(seconds: 3));
        revoked = true;
      } on PushxApiException catch (error) {
        revoked = error.unauthorized;
        if (!revoked) errors.add('remote_revoke_failed');
      } on Object {
        errors.add('remote_revoke_failed');
      } finally {
        try {
          api?.close();
        } on Object {
          errors.add('api_close_failed');
        }
      }
    }
    // Persist proof before deleting the credential. A later local cleanup
    // failure must not turn a known successful revocation back into unknown.
    var proofSaved = true;
    if (revoked && !progress.remoteRevoked) {
      proofSaved = await attempt(
        'revocation_commit_failed',
        () => store.completeRevocation(s.binding.scope, s.binding.bindingId),
      );
      local = proofSaved && local;
    }
    try {
      final ids = await store.notificationIds(s);
      final deadline = DateTime.now().add(const Duration(seconds: 3));
      for (var offset = 0; offset < ids.length; offset += 8) {
        final remaining = deadline.difference(DateTime.now());
        if (remaining <= Duration.zero) throw TimeoutException('cleanup');
        // No outer abandoned loop: once this wave times out, no more IDs start.
        // Successful members commit independently, even if another member fails.
        await Future.wait(
          ids.skip(offset).take(8).map((id) async {
            await presenter.cancel(id);
            await store.confirmNotificationCanceled(s, id);
          }),
        ).timeout(remaining);
      }
    } on Object {
      errors.add('notification_cleanup_failed');
      local = false;
    }
    if (!progress.credentialDeleted && proofSaved) {
      local =
          await attempt('credential_delete_failed', () async {
            await credentials.delete(s.binding.credentialKey);
            await store.completeCleanupStep(s, 'credential_deleted');
          }) &&
          local;
    }
    if (local) {
      local = await attempt('cleanup_commit_failed', () async {
        revoked = await store.completeCleanup(s, remoteRevoked: revoked);
      });
    }
    return PushxStopResult(
      revoked,
      cleanupComplete: local,
      errorCodes: List.unmodifiable(errors),
    );
  }

  Future<void> recover() async {
    for (final s in await store.readPendingCleanup()) {
      await cleanup(s);
    }
  }
}
