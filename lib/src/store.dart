import 'dart:convert';

import 'package:path/path.dart' as path;
import 'package:sqflite/sqflite.dart';

import 'config.dart';
import 'protocol.dart';

class PushxInboxItem {
  const PushxInboxItem(this.packet, this.systemId, this.state);
  final PushxPacket packet;
  final int systemId;
  final String state;
}

class PushxQueuedAck {
  const PushxQueuedAck(this.ack, this.version);
  final PushxAck ack;
  final int version;
}

class PushxCleanupProgress {
  const PushxCleanupProgress({
    this.remoteRevoked = false,
    this.serviceStopped = false,
    this.credentialDeleted = false,
  });
  final bool remoteRevoked, serviceStopped, credentialDeleted;
}

/// Each isolate opens its own connection. Transactions and revisions, rather
/// than Dart singletons, protect the active identity and durable ACK queue.
class PushxStore {
  PushxStore._(this._db, this._factory);
  final Database _db;
  final DatabaseFactory _factory;
  String get databasePath => _db.path;

  // Native SQLite workers may serialize connections. A long busy_timeout can
  // block the worker needed to finish the transaction holding the lock.
  // Retry rolled-back, DB-only transactions asynchronously instead.
  Future<T> _transaction<T>(Future<T> Function(Transaction tx) work) async {
    for (var attempt = 0; ; attempt++) {
      try {
        return await _db.transaction(work);
      } on DatabaseException catch (error) {
        final code = error.getResultCode();
        final busy = code != null && ((code & 255) == 5 || (code & 255) == 6);
        if (!busy || attempt >= 19) rethrow;
        await Future<void>.delayed(
          Duration(milliseconds: attempt < 5 ? 10 * (attempt + 1) : 200),
        );
      }
    }
  }

  static Future<PushxStore> open({
    String? databasePath,
    DatabaseFactory? factory,
  }) async {
    final f = factory ?? databaseFactory;
    final dbPath =
        databasePath ?? path.join(await f.getDatabasesPath(), 'pushx.db');
    final db = await f.openDatabase(
      dbPath,
      options: OpenDatabaseOptions(
        version: 5,
        singleInstance: false,
        onConfigure: (db) async {
          await db.rawQuery('PRAGMA busy_timeout = 25');
          await db.rawQuery('PRAGMA journal_mode = WAL');
        },
        onUpgrade: (db, oldVersion, _) async {
          if (oldVersion < 2) {
            await db.execute(
              'ALTER TABLE inbox ADD COLUMN opened INTEGER NOT NULL DEFAULT 0',
            );
          }
          if (oldVersion < 3) await _createCleanup(db);
          if (oldVersion < 4) {
            await db.execute(
              'ALTER TABLE runtime ADD COLUMN stop_result INTEGER NOT NULL DEFAULT -1',
            );
          }
          if (oldVersion < 5) {
            final runtimeColumns = (await db.rawQuery(
              'PRAGMA table_info(runtime)',
            )).map((r) => r['name']).toSet();
            if (!runtimeColumns.contains('runtime_owner')) {
              await db.execute(
                "ALTER TABLE runtime ADD COLUMN runtime_owner TEXT NOT NULL DEFAULT ''",
              );
            }
            final cleanupColumns = (await db.rawQuery(
              'PRAGMA table_info(pending_cleanup)',
            )).map((r) => r['name']).toSet();
            for (final column in [
              'remote_revoked',
              'service_stopped',
              'credential_deleted',
            ]) {
              if (!cleanupColumns.contains(column)) {
                await db.execute(
                  'ALTER TABLE pending_cleanup ADD COLUMN $column INTEGER NOT NULL DEFAULT 0',
                );
              }
            }
            await db.execute(
              'UPDATE pending_cleanup SET remote_revoked=1 WHERE NOT EXISTS(SELECT 1 FROM pending_revocations r WHERE r.scope=pending_cleanup.scope AND r.binding=pending_cleanup.binding)',
            );
            await _createNotifications(db);
            // Include presenting rows: the OS may have accepted show() before
            // a crashed engine committed the posted stage.
            await db.execute(
              'INSERT OR IGNORE INTO native_notifications(scope,binding,event_key,system_id) SELECT scope,binding,event_key,system_id FROM inbox WHERE state IN (\'presenting\',\'posted\')',
            );
          }
        },
        onCreate: (db, _) async {
          await db.execute(
            "CREATE TABLE runtime (id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL, enabled INTEGER NOT NULL, config TEXT NOT NULL, binding TEXT NOT NULL, stop_result INTEGER NOT NULL DEFAULT -1, runtime_owner TEXT NOT NULL DEFAULT '')",
          );
          await db.execute(
            'CREATE TABLE inbox (scope TEXT NOT NULL, binding TEXT NOT NULL, id TEXT NOT NULL, event_key TEXT NOT NULL, packet TEXT NOT NULL, system_id INTEGER NOT NULL, expires INTEGER NOT NULL, state TEXT NOT NULL, reason TEXT NOT NULL DEFAULT \'\', lease_owner TEXT, lease_until INTEGER, opened INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(scope,binding,id))',
          );
          await db.execute(
            'CREATE INDEX inbox_pending ON inbox(scope,binding,state,expires)',
          );
          await db.execute(
            'CREATE INDEX inbox_event ON inbox(scope,binding,event_key)',
          );
          await db.execute('CREATE INDEX inbox_system_id ON inbox(system_id)');
          await db.execute(
            'CREATE TABLE ack_outbox (scope TEXT NOT NULL, binding TEXT NOT NULL, id TEXT NOT NULL, stage TEXT NOT NULL, reason TEXT NOT NULL, version INTEGER NOT NULL, PRIMARY KEY(scope,binding,id))',
          );
          await db.execute(
            'CREATE TABLE pending_opens (open_id INTEGER PRIMARY KEY AUTOINCREMENT, scope TEXT NOT NULL, binding TEXT NOT NULL, id TEXT NOT NULL, UNIQUE(scope,binding,id))',
          );
          await db.execute(
            'CREATE TABLE status (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL)',
          );
          await db.execute(
            'CREATE TABLE pending_revocations (scope TEXT NOT NULL, binding TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(scope,binding))',
          );
          await _createCleanup(db);
          await _createNotifications(db);
        },
      ),
    );
    return PushxStore._(db, f);
  }

  static Future<void> _createCleanup(DatabaseExecutor db) => db.execute(
    'CREATE TABLE IF NOT EXISTS pending_cleanup (scope TEXT NOT NULL, binding TEXT NOT NULL, value TEXT NOT NULL, remote_revoked INTEGER NOT NULL DEFAULT 0, service_stopped INTEGER NOT NULL DEFAULT 0, credential_deleted INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(scope,binding))',
  );

  static Future<void> _createNotifications(DatabaseExecutor db) => db.execute(
    'CREATE TABLE IF NOT EXISTS native_notifications (scope TEXT NOT NULL, binding TEXT NOT NULL, event_key TEXT NOT NULL, system_id INTEGER PRIMARY KEY, UNIQUE(scope,binding,event_key))',
  );

  /// Stop intent and recovery records commit together, before external effects.
  Future<bool> beginStop(PushxSnapshot expected, {bool revoked = false}) =>
      _transaction((tx) async {
        final current = await _snapshot(tx);
        if (current == null ||
            current.revision != expected.revision ||
            current.runtimeOwner != expected.runtimeOwner ||
            current.binding.scope != expected.binding.scope ||
            current.binding.bindingId != expected.binding.bindingId) {
          return false;
        }
        if (current.enabled) {
          await tx.update('runtime', {
            'enabled': 0,
            'revision': current.revision + 1,
          }, where: 'id=1');
        }
        await tx.delete(
          'pending_opens',
          where: 'scope=? AND binding=?',
          whereArgs: _scope(expected),
        );
        await tx.insert('pending_cleanup', {
          'scope': expected.binding.scope,
          'binding': expected.binding.bindingId,
          'value': jsonEncode({
            'config': expected.config.toJson(),
            'binding': expected.binding.toJson(),
          }),
        }, conflictAlgorithm: ConflictAlgorithm.ignore);
        if (revoked) {
          await tx.update(
            'pending_cleanup',
            {'remote_revoked': 1},
            where: 'scope=? AND binding=?',
            whereArgs: _scope(expected),
          );
          await tx.delete(
            'pending_revocations',
            where: 'scope=? AND binding=?',
            whereArgs: _scope(expected),
          );
        } else if ((await tx.query(
              'pending_cleanup',
              where: 'scope=? AND binding=?',
              whereArgs: _scope(expected),
            )).single['remote_revoked'] !=
            1) {
          await _revocation(tx, expected);
        }
        return true;
      });

  Future<List<PushxSnapshot>> readPendingCleanup() async =>
      (await _db.query('pending_cleanup')).map((row) {
        final value = jsonObject(jsonDecode(row['value'] as String));
        return PushxSnapshot(
          PushxConfig.fromJson(jsonObject(value['config'])),
          PushxBinding.fromJson(jsonObject(value['binding'])),
          0,
          false,
        );
      }).toList();

  Future<PushxCleanupProgress?> cleanupProgress(PushxSnapshot s) async {
    final rows = await _db.query(
      'pending_cleanup',
      where: 'scope=? AND binding=?',
      whereArgs: _scope(s),
    );
    if (rows.isEmpty) return null;
    final row = rows.single;
    return PushxCleanupProgress(
      remoteRevoked: row['remote_revoked'] == 1,
      serviceStopped: row['service_stopped'] == 1,
      credentialDeleted: row['credential_deleted'] == 1,
    );
  }

  Future<void> completeCleanupStep(PushxSnapshot s, String step) {
    if (!['service_stopped', 'credential_deleted'].contains(step)) {
      throw ArgumentError('Invalid cleanup step');
    }
    return _transaction((tx) async {
      await tx.update(
        'pending_cleanup',
        {step: 1},
        where: 'scope=? AND binding=?',
        whereArgs: _scope(s),
      );
    });
  }

  Future<bool?> completedStop(PushxSnapshot s) async {
    final current = await snapshot();
    if (current == null ||
        current.enabled ||
        current.binding.credentialKey != s.binding.credentialKey) {
      return null;
    }
    final rows = await _db.query(
      'runtime',
      columns: ['stop_result'],
      where: 'id=1',
    );
    final value = rows.single['stop_result'] as int;
    return value < 0 ? null : value == 1;
  }

  Future<bool> completeCleanup(PushxSnapshot s, {bool remoteRevoked = false}) =>
      _transaction((tx) async {
        final progress = await tx.query(
          'pending_cleanup',
          where: 'scope=? AND binding=?',
          whereArgs: _scope(s),
        );
        final current = await _snapshot(tx);
        final stop = await tx.query(
          'runtime',
          columns: ['stop_result'],
          where: 'id=1',
        );
        // Host reconciliation or another cleanup may have committed proof
        // after this caller read its progress. Completion must be monotonic.
        final confirmed =
            remoteRevoked ||
            (progress.isNotEmpty && progress.single['remote_revoked'] == 1) ||
            (current != null &&
                current.binding.scope == s.binding.scope &&
                current.binding.bindingId == s.binding.bindingId &&
                stop.single['stop_result'] == 1);
        if (current != null &&
            current.binding.credentialKey == s.binding.credentialKey) {
          if (current.enabled) {
            throw StateError('Cannot clean an enabled binding');
          }
          await tx.update('runtime', {
            'stop_result': confirmed ? 1 : 0,
          }, where: 'id=1');
        }
        for (final table in [
          'pending_opens',
          'ack_outbox',
          'inbox',
          'native_notifications',
        ]) {
          await tx.delete(
            table,
            where: 'scope=? AND binding=?',
            whereArgs: _scope(s),
          );
        }
        await tx.delete(
          'pending_cleanup',
          where: 'scope=? AND binding=?',
          whereArgs: _scope(s),
        );
        return confirmed;
      });

  /// Never evicts an enabled identity or records still needed for cleanup.
  Future<bool> purgeBinding(PushxSnapshot s) => _transaction((tx) async {
    final current = await _snapshot(tx);
    if (current != null &&
        current.enabled &&
        current.binding.scope == s.binding.scope &&
        current.binding.bindingId == s.binding.bindingId) {
      return false;
    }
    if ((await tx.query(
      'pending_cleanup',
      where: 'scope=? AND binding=?',
      whereArgs: _scope(s),
    )).isNotEmpty) {
      return false;
    }
    if ((await tx.query(
      'native_notifications',
      where: 'scope=? AND binding=?',
      whereArgs: _scope(s),
      limit: 1,
    )).isNotEmpty) {
      return false;
    }
    for (final table in ['pending_opens', 'ack_outbox', 'inbox']) {
      await tx.delete(
        table,
        where: 'scope=? AND binding=?',
        whereArgs: _scope(s),
      );
    }
    return true;
  });

  Future<PushxSnapshot?> _snapshot(DatabaseExecutor db) async {
    final rows = await db.query('runtime', where: 'id=1');
    if (rows.isEmpty) return null;
    final r = rows.single;
    return PushxSnapshot(
      PushxConfig.fromJson(jsonObject(jsonDecode(r['config'] as String))),
      PushxBinding.fromJson(jsonObject(jsonDecode(r['binding'] as String))),
      r['revision'] as int,
      r['enabled'] == 1,
      runtimeOwner: r['runtime_owner'] as String,
    );
  }

  Future<PushxSnapshot?> snapshot() => _snapshot(_db);

  /// An uncancellable native show() may finish after its engine closed this
  /// connection. Read ownership through a short-lived connection before cancel.
  Future<PushxSnapshot?> snapshotForCleanup() async {
    if (_db.isOpen) {
      try {
        return await snapshot();
      } on DatabaseException {
        if (_db.isOpen) rethrow;
      }
    }
    final reader = await PushxStore.open(
      databasePath: databasePath,
      factory: _factory,
    );
    try {
      return await reader.snapshot();
    } finally {
      await reader.close();
    }
  }

  /// Claim once per engine, independently from configuration revisions.
  Future<PushxSnapshot> claimRuntime(String owner) => _transaction((tx) async {
    final current = await _snapshot(tx);
    if (owner.isEmpty || current == null || !current.enabled) {
      throw StateError('Pushx binding is disabled');
    }
    await tx.update('runtime', {
      'runtime_owner': owner,
      'revision': current.revision + 1,
    }, where: 'id=1');
    return (await _snapshot(tx))!;
  });

  Future<bool> _current(DatabaseExecutor db, PushxSnapshot expected) async {
    final current = await _snapshot(db);
    return current != null &&
        current.enabled &&
        current.revision == expected.revision &&
        current.runtimeOwner == expected.runtimeOwner &&
        current.binding.scope == expected.binding.scope &&
        current.binding.bindingId == expected.binding.bindingId;
  }

  Future<bool> isCurrent(PushxSnapshot expected) => _current(_db, expected);

  Future<bool> disable(PushxSnapshot expected) => _transaction((tx) async {
    final current = await _snapshot(tx);
    if (current == null ||
        current.revision != expected.revision ||
        current.binding.scope != expected.binding.scope ||
        current.binding.bindingId != expected.binding.bindingId) {
      return false;
    }
    await tx.update('runtime', {
      'enabled': 0,
      'revision': current.revision + 1,
    }, where: 'id=1');
    await tx.delete(
      'pending_opens',
      where: 'scope=? AND binding=?',
      whereArgs: _scope(expected),
    );
    return true;
  });

  Future<void> activate(PushxConfig config, PushxBinding binding) =>
      _transaction((tx) async {
        final old = await _snapshot(tx);
        await tx.insert('runtime', {
          'id': 1,
          'revision': (old?.revision ?? 0) + 1,
          'enabled': 0,
          'config': jsonEncode(config.toJson()),
          'binding': jsonEncode(binding.toJson()),
        }, conflictAlgorithm: ConflictAlgorithm.replace);
        await tx.delete('pending_opens');
        await tx.delete('status');
      });

  Future<PushxSnapshot> setEnabled(bool enabled) => _transaction((tx) async {
    final old = await _snapshot(tx);
    if (old == null) throw StateError('Activate a Pushx binding first');
    await tx.update('runtime', {
      'enabled': enabled ? 1 : 0,
      'revision': old.revision + 1,
      if (enabled) 'runtime_owner': '',
    }, where: 'id=1');
    if (!enabled) await tx.delete('pending_opens');
    return (await _snapshot(tx))!;
  });

  Future<bool> invalidate(PushxSnapshot expected) => _transaction((tx) async {
    if (!(await _current(tx, expected))) return false;
    await tx.update('runtime', {
      'enabled': 0,
      'revision': expected.revision + 1,
    }, where: 'id=1');
    await tx.delete(
      'pending_opens',
      where: 'scope=? AND binding=?',
      whereArgs: [expected.binding.scope, expected.binding.bindingId],
    );
    return true;
  });

  Future<void> updateConfig(PushxConfig config) => _transaction((tx) async {
    final old = await _snapshot(tx);
    if (old == null) throw StateError('Activate a Pushx binding first');
    if (old.config.hub != config.hub) {
      throw StateError('Hub changes require a new binding');
    }
    await tx.update('runtime', {
      'config': jsonEncode(config.toJson()),
      'revision': old.revision + 1,
    }, where: 'id=1');
  });

  Future<void> extendCredential(PushxSnapshot expected, int expiresAt) =>
      _transaction((tx) async {
        if (!(await _current(tx, expected))) return;
        final binding = {...expected.binding.toJson(), 'expiresAt': expiresAt};
        await tx.update('runtime', {
          'binding': jsonEncode(binding),
        }, where: 'id=1');
      });

  List<Object?> _scope(PushxSnapshot s) => [
    s.binding.scope,
    s.binding.bindingId,
  ];

  Future<void> _queueAck(
    DatabaseExecutor tx,
    PushxSnapshot s,
    PushxAck ack,
  ) async {
    ack.validate();
    await tx.rawInsert(
      '''INSERT INTO ack_outbox(scope,binding,id,stage,reason,version)
      VALUES(?,?,?,?,?,1) ON CONFLICT(scope,binding,id) DO UPDATE SET
      stage=excluded.stage, reason=excluded.reason, version=ack_outbox.version+1
      WHERE ack_outbox.stage='received' ''',
      [..._scope(s), ack.notificationId, ack.stage.name, ack.reason],
    );
  }

  Future<bool> accept(
    PushxSnapshot s,
    PushxPacket packet,
    int now, {
    String? blockedReason,
  }) => _transaction((tx) async {
    if (!(await _current(tx, s))) return false;
    if (packet.bindingId != s.binding.bindingId ||
        packet.subscriptionId != s.binding.subscriptionId) {
      return false;
    }
    final existing = await tx.query(
      'inbox',
      where: 'scope=? AND binding=? AND event_key=?',
      whereArgs: [..._scope(s), packet.eventKey],
      limit: 1,
    );
    final same = await tx.query(
      'inbox',
      where: 'scope=? AND binding=? AND id=?',
      whereArgs: [..._scope(s), packet.notificationId],
      limit: 1,
    );
    if (same.isNotEmpty) {
      final row = same.single;
      final stage = row['state'] == 'posted'
          ? PushxAckStage.posted
          : row['state'] == 'blocked'
          ? PushxAckStage.blocked
          : PushxAckStage.received;
      await _queueAck(
        tx,
        s,
        PushxAck(
          packet.notificationId,
          stage,
          reason: stage == PushxAckStage.blocked ? row['reason'] as String : '',
        ),
      );
      return true;
    }
    final count =
        Sqflite.firstIntValue(
          await tx.rawQuery(
            'SELECT COUNT(*) FROM inbox WHERE scope=? AND binding=?',
            _scope(s),
          ),
        ) ??
        0;
    if (count >= s.config.maxInboxItems) {
      throw StateError('Pushx inbox is full');
    }
    int systemId;
    if (existing.isNotEmpty) {
      systemId = existing.first['system_id'] as int;
    } else {
      final registered = await tx.query(
        'native_notifications',
        where: 'scope=? AND binding=? AND event_key=?',
        whereArgs: [..._scope(s), packet.eventKey],
      );
      systemId = _stableId(
        '${s.binding.scope}:${s.binding.bindingId}:${packet.eventKey}',
      );
      if (registered.isNotEmpty) {
        systemId = registered.single['system_id'] as int;
      } else {
        while ((await tx.rawQuery(
          'SELECT system_id FROM inbox WHERE system_id=? UNION SELECT system_id FROM native_notifications WHERE system_id=? LIMIT 1',
          [systemId, systemId],
        )).isNotEmpty) {
          systemId = systemId == 2147483646 ? 10000 : systemId + 1;
        }
      }
    }
    final original = existing.isEmpty ? null : existing.first;
    final reason =
        blockedReason ??
        (packet.expiresAt <= now
            ? 'delivery_expired'
            : original?['reason'] as String? ?? '');
    final state = reason.isNotEmpty
        ? 'blocked'
        : original?['state'] as String? ?? 'received';
    await tx.insert('inbox', {
      'scope': s.binding.scope,
      'binding': s.binding.bindingId,
      'id': packet.notificationId,
      'event_key': packet.eventKey,
      'packet': jsonEncode(packet.toJson()),
      'system_id': systemId,
      'expires': packet.expiresAt,
      'state': state,
      'reason': reason,
      'lease_owner': original?['lease_owner'],
      'lease_until': original?['lease_until'],
    });
    final stage = state == 'posted'
        ? PushxAckStage.posted
        : state == 'blocked'
        ? PushxAckStage.blocked
        : PushxAckStage.received;
    await _queueAck(
      tx,
      s,
      PushxAck(
        packet.notificationId,
        stage,
        reason: stage == PushxAckStage.blocked ? reason : '',
      ),
    );
    return true;
  });

  Future<void> reject(PushxSnapshot s, String notificationId, String reason) =>
      _transaction((tx) async {
        if (await _current(tx, s)) {
          await _queueAck(
            tx,
            s,
            PushxAck(notificationId, PushxAckStage.blocked, reason: reason),
          );
        }
      });

  static int _stableId(String value) {
    var hash = 0;
    for (final byte in utf8.encode(value)) {
      hash = (hash * 31 + byte) & 0x7fffffff;
    }
    return 10000 + hash % (2147483646 - 10000);
  }

  Future<List<PushxInboxItem>> pending(PushxSnapshot s, int now) async {
    final rows = await _db.rawQuery(
      '''SELECT * FROM inbox WHERE scope=? AND binding=?
      AND (state='received' OR (state='presenting' AND lease_until<=?))
      GROUP BY event_key ORDER BY rowid LIMIT 50''',
      [..._scope(s), now],
    );
    return rows
        .map(
          (r) => PushxInboxItem(
            PushxPacket.fromJson(jsonObject(jsonDecode(r['packet'] as String))),
            r['system_id'] as int,
            r['state'] as String,
          ),
        )
        .toList();
  }

  Future<bool> claim(
    PushxSnapshot s,
    PushxInboxItem item,
    String owner,
    int now,
  ) => _transaction((tx) async {
    if (!(await _current(tx, s))) return false;
    final claimed =
        await tx.rawUpdate(
          '''UPDATE inbox SET state='presenting',lease_owner=?,lease_until=?
      WHERE scope=? AND binding=? AND event_key=?
      AND (state='received' OR (state='presenting' AND lease_until<=?))''',
          [owner, now + 30000, ..._scope(s), item.packet.eventKey, now],
        ) >
        0;
    if (claimed) {
      // Commit the minimal native ID before show(), independently of inbox TTL.
      await tx.insert('native_notifications', {
        'scope': s.binding.scope,
        'binding': s.binding.bindingId,
        'event_key': item.packet.eventKey,
        'system_id': item.systemId,
      }, conflictAlgorithm: ConflictAlgorithm.ignore);
    }
    return claimed;
  });

  Future<bool> finish(
    PushxSnapshot s,
    PushxInboxItem item,
    String owner, {
    String reason = '',
  }) => _transaction((tx) async {
    if (!(await _current(tx, s))) return false;
    final stage = reason.isEmpty ? PushxAckStage.posted : PushxAckStage.blocked;
    final rows = await tx.query(
      'inbox',
      where:
          'scope=? AND binding=? AND event_key=? AND lease_owner=? AND state=\'presenting\'',
      whereArgs: [..._scope(s), item.packet.eventKey, owner],
    );
    for (final row in rows) {
      await _queueAck(
        tx,
        s,
        PushxAck(row['id'] as String, stage, reason: reason),
      );
    }
    return await tx.update(
          'inbox',
          {
            'state': stage.name,
            'reason': reason,
            'lease_owner': null,
            'lease_until': null,
          },
          where:
              'scope=? AND binding=? AND event_key=? AND lease_owner=? AND state=\'presenting\'',
          whereArgs: [..._scope(s), item.packet.eventKey, owner],
        ) >
        0;
  });

  Future<List<PushxQueuedAck>> acks(PushxSnapshot s) async {
    final rows = await _db.query(
      'ack_outbox',
      where: 'scope=? AND binding=?',
      whereArgs: _scope(s),
      limit: pushxMaxBatchSize,
    );
    return rows
        .map(
          (r) => PushxQueuedAck(
            PushxAck(
              r['id'] as String,
              PushxAckStage.values.byName(r['stage'] as String),
              reason: r['reason'] as String,
            ),
            r['version'] as int,
          ),
        )
        .toList();
  }

  Future<void> confirmAcks(PushxSnapshot s, List<PushxQueuedAck> sent) =>
      _transaction((tx) async {
        for (final a in sent) {
          await tx.delete(
            'ack_outbox',
            where: 'scope=? AND binding=? AND id=? AND version=?',
            whereArgs: [..._scope(s), a.ack.notificationId, a.version],
          );
        }
      });

  Future<List<int>> notificationIds(
    PushxSnapshot s,
  ) async => (await _db.rawQuery(
    'SELECT system_id FROM inbox WHERE scope=? AND binding=? UNION SELECT system_id FROM native_notifications WHERE scope=? AND binding=? ORDER BY system_id',
    [..._scope(s), ..._scope(s)],
  )).map((r) => r['system_id'] as int).toList();

  /// Persist each successful cancellation. Recovery never repeats a completed
  /// prefix; retries can safely repeat an OS cancellation if the DB commit fails.
  Future<void> confirmNotificationCanceled(PushxSnapshot s, int systemId) =>
      _transaction((tx) async {
        final current = await _snapshot(tx);
        if (current != null &&
            current.enabled &&
            current.binding.scope == s.binding.scope &&
            current.binding.bindingId == s.binding.bindingId) {
          return;
        }
        if ((await tx.query(
          'pending_cleanup',
          where: 'scope=? AND binding=?',
          whereArgs: _scope(s),
        )).isEmpty) {
          return;
        }
        for (final table in ['inbox', 'native_notifications']) {
          await tx.delete(
            table,
            where: 'scope=? AND binding=? AND system_id=?',
            whereArgs: [..._scope(s), systemId],
          );
        }
      });

  Future<void> recordOpen(String? payload, int now) async {
    if (payload == null || payload.length > 2048) return;
    Json target;
    try {
      target = jsonObject(jsonDecode(payload));
    } on Object {
      return;
    }
    await _transaction((tx) async {
      final s = await _snapshot(tx);
      if (s == null ||
          !s.enabled ||
          target['scope'] != s.binding.scope ||
          target['bindingId'] != s.binding.bindingId) {
        return;
      }
      final status = await tx.query('status', where: 'id=1');
      final clock = status.isEmpty
          ? <String, dynamic>{}
          : jsonObject(jsonDecode(status.single['value'] as String));
      final effectiveNow =
          now +
          (clock['serverOffset'] is int ? clock['serverOffset'] as int : 0);
      final rows = await tx.query(
        'inbox',
        where:
            'scope=? AND binding=? AND id=? AND expires>? AND opened=0 AND state IN (\'posted\',\'presenting\')',
        whereArgs: [..._scope(s), target['notificationId'], effectiveNow],
        limit: 1,
      );
      if (rows.isEmpty) return;
      await tx.insert('pending_opens', {
        'scope': s.binding.scope,
        'binding': s.binding.bindingId,
        'id': target['notificationId'],
      }, conflictAlgorithm: ConflictAlgorithm.ignore);
    });
  }

  Future<List<PushxOpen>> readPendingOpens(int now) async {
    final s = await snapshot();
    if (s == null || !s.enabled) return [];
    final rows = await _db.rawQuery(
      '''SELECT o.open_id,i.packet FROM pending_opens o JOIN inbox i
      ON o.scope=i.scope AND o.binding=i.binding AND o.id=i.id
      WHERE o.scope=? AND o.binding=? AND i.expires>? ORDER BY o.open_id''',
      [..._scope(s), now],
    );
    return rows
        .map(
          (r) => PushxOpen(
            r['open_id'] as int,
            s.binding.scope,
            PushxPacket.fromJson(jsonObject(jsonDecode(r['packet'] as String))),
          ),
        )
        .toList();
  }

  Future<void> completeOpen(int id) => _transaction((tx) async {
    final rows = await tx.query(
      'pending_opens',
      where: 'open_id=?',
      whereArgs: [id],
    );
    if (rows.isNotEmpty) {
      final row = rows.single;
      await tx.update(
        'inbox',
        {'opened': 1},
        where: 'scope=? AND binding=? AND id=?',
        whereArgs: [row['scope'], row['binding'], row['id']],
      );
      await tx.delete('pending_opens', where: 'open_id=?', whereArgs: [id]);
    }
  });

  Future<void> writeStatus(PushxSnapshot s, Json status) =>
      _transaction((tx) async {
        if (!(await _current(tx, s))) return;
        await tx.insert('status', {
          'id': 1,
          'value': jsonEncode(status),
        }, conflictAlgorithm: ConflictAlgorithm.replace);
      });
  Future<Json> status() async {
    final rows = await _db.query('status', where: 'id=1');
    return rows.isEmpty
        ? {}
        : jsonObject(jsonDecode(rows.single['value'] as String));
  }

  Future<void> _revocation(DatabaseExecutor db, PushxSnapshot s) async {
    await db.insert('pending_revocations', {
      'scope': s.binding.scope,
      'binding': s.binding.bindingId,
      'value': jsonEncode({
        'scope': s.binding.scope,
        'hub': s.config.hub.toString(),
        'subscriptionId': s.binding.subscriptionId,
        'bindingId': s.binding.bindingId,
      }),
    }, conflictAlgorithm: ConflictAlgorithm.replace);
  }

  Future<void> pendingRevocation(PushxSnapshot s) =>
      _transaction((tx) => _revocation(tx, s));

  Future<List<Json>> readPendingRevocations() async => (await _db.query(
    'pending_revocations',
  )).map((r) => jsonObject(jsonDecode(r['value'] as String))).toList();
  Future<void> completeRevocation(String scope, String bindingId) =>
      _transaction((tx) async {
        await tx.update(
          'pending_cleanup',
          {'remote_revoked': 1},
          where: 'scope=? AND binding=?',
          whereArgs: [scope, bindingId],
        );
        await tx.delete(
          'pending_revocations',
          where: 'scope=? AND binding=?',
          whereArgs: [scope, bindingId],
        );
        final current = await _snapshot(tx);
        if (current != null &&
            !current.enabled &&
            current.binding.scope == scope &&
            current.binding.bindingId == bindingId) {
          await tx.rawUpdate(
            'UPDATE runtime SET stop_result=1 WHERE id=1 AND stop_result=0',
          );
        }
      });

  Future<void> prune(int now, {int retentionDays = 7}) => _transaction((
    tx,
  ) async {
    final cutoff = now - Duration(days: retentionDays).inMilliseconds;
    await tx.rawDelete(
      'DELETE FROM ack_outbox WHERE EXISTS(SELECT 1 FROM inbox i WHERE i.scope=ack_outbox.scope AND i.binding=ack_outbox.binding AND i.id=ack_outbox.id AND i.expires<?)',
      [cutoff],
    );
    await tx.rawDelete(
      'DELETE FROM pending_opens WHERE EXISTS(SELECT 1 FROM inbox i WHERE i.scope=pending_opens.scope AND i.binding=pending_opens.binding AND i.id=pending_opens.id AND i.expires<?)',
      [now],
    );
    // Retain system notification IDs until interrupted cleanup can cancel them.
    await tx.rawDelete(
      'DELETE FROM inbox WHERE expires<? AND NOT EXISTS(SELECT 1 FROM pending_cleanup c WHERE c.scope=inbox.scope AND c.binding=inbox.binding)',
      [cutoff],
    );
  });

  Future<void> close() => _db.close();
}
