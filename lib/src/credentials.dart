import 'package:flutter_secure_storage/flutter_secure_storage.dart';

abstract interface class PushxCredentialStore {
  Future<String?> read(String key);
  Future<void> write(String key, String token);
  Future<void> delete(String key);
}

class PushxSecureCredentials implements PushxCredentialStore {
  PushxSecureCredentials({FlutterSecureStorage? storage})
    : _storage =
          storage ??
          const FlutterSecureStorage(
            aOptions: AndroidOptions(storageNamespace: 'pushx'),
          );
  final FlutterSecureStorage _storage;
  @override
  Future<String?> read(String key) => _storage.read(key: key);
  @override
  Future<void> write(String key, String token) =>
      _storage.write(key: key, value: token);
  @override
  Future<void> delete(String key) => _storage.delete(key: key);
}
