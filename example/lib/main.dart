import 'dart:async';
import 'dart:convert';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:http/http.dart' as http;
import 'package:pushx_flutter/pushx_flutter.dart';

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(const MaterialApp(home: PushxExample()));
}

class PushxExample extends StatefulWidget {
  const PushxExample({super.key});
  @override
  State<PushxExample> createState() => _PushxExampleState();
}

class _PushxExampleState extends State<PushxExample> {
  final _hub = TextEditingController(text: 'http://127.0.0.1:8099/');
  final _bootstrap = TextEditingController();
  Pushx? _sdk;
  Timer? _timer;
  bool _busy = false;
  bool _refreshing = false;
  String _message = '输入 Go 示例的连接地址和开发口令';
  Json _status = {};

  @override
  void initState() {
    super.initState();
    _busy = true;
    unawaited(_restore());
    _timer = Timer.periodic(
      const Duration(seconds: 2),
      (_) => unawaited(_refresh()),
    );
  }

  Future<void> _restore() async {
    try {
      final store = await PushxStore.open();
      final saved = await store.snapshot();
      await store.close();
      if (saved != null) {
        _hub.text = saved.config.hub.toString();
        _sdk = await Pushx.initialize(config: saved.config);
        await _refresh();
      }
    } on Object catch (error, stack) {
      debugPrint('Pushx restore failed: ${error.runtimeType}');
      debugPrintStack(stackTrace: stack, maxFrames: 8);
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  Future<Pushx> _initialize() async {
    return _sdk ??= await Pushx.initialize(
      config: PushxConfig(
        hub: Uri.parse(_hub.text.trim()),
        allowInsecure: true,
      ),
    );
  }

  Future<Json> _admin(String route) async {
    final base = Uri.parse(_hub.text.trim());
    final response = await http
        .post(
          base.resolve(route),
          headers: {'Authorization': 'Bearer ${_bootstrap.text}'},
        )
        .timeout(const Duration(seconds: 10));
    if (response.statusCode != 200) {
      throw StateError('示例 API 失败：${response.statusCode}');
    }
    return jsonObject(jsonDecode(response.body));
  }

  Future<void> _run(Future<void> Function() action) async {
    if (_busy) return;
    setState(() => _busy = true);
    try {
      await action();
      await _refresh();
    } on Object catch (error, stack) {
      debugPrint(
        'Pushx example failed: ${error.runtimeType}${error is PlatformException ? '/${error.code}' : ''}',
      );
      debugPrintStack(stackTrace: stack, maxFrames: 8);
      if (mounted) {
        setState(
          () => _message = error is StateError
              ? error.message.toString()
              : '操作失败，请检查服务、权限和 Android 配置',
        );
      }
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  Future<void> _refresh() async {
    final sdk = _sdk;
    if (sdk == null || !mounted || _refreshing) return;
    _refreshing = true;
    try {
      final status = await sdk.snapshot();
      for (final event in await sdk.readPendingOpens()) {
        if (mounted) {
          setState(
            () => _message =
                '已打开通知：${event.packet.kind} / ${event.packet.notificationId}',
          );
        }
        await sdk.completeOpen(event.id);
      }
      if (mounted) setState(() => _status = status);
    } finally {
      _refreshing = false;
    }
  }

  @override
  Widget build(BuildContext context) => Scaffold(
    appBar: AppBar(title: const Text('Pushx SDK 示例')),
    body: SingleChildScrollView(
      padding: const EdgeInsets.all(20),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          TextField(
            controller: _hub,
            enabled: _sdk == null && !_busy,
            decoration: const InputDecoration(labelText: 'Go 示例地址'),
          ),
          TextField(
            controller: _bootstrap,
            obscureText: true,
            decoration: const InputDecoration(
              labelText: 'PUSHX_DEMO_TOKEN 开发口令',
            ),
          ),
          const SizedBox(height: 16),
          FilledButton(
            onPressed: _busy
                ? null
                : () => _run(() async {
                    final sdk = await _initialize();
                    await sdk.requestPermission();
                    final registration = await _admin('demo/register');
                    await sdk.activate(
                      scope: jsonString(registration, 'scope'),
                      subscriptionId: jsonString(
                        registration,
                        'subscriptionId',
                      ),
                      bindingId: jsonString(registration, 'bindingId'),
                      credential: jsonString(registration, 'credential'),
                      expiresAt: jsonInt(registration, 'expiresAt'),
                    );
                    await sdk.start();
                    if (mounted) setState(() => _message = '后台服务已启动');
                  }),
            child: const Text('登记并开启后台推送'),
          ),
          OutlinedButton(
            onPressed: _busy
                ? null
                : () => _run(() async {
                    await _admin('demo/send');
                    if (mounted) setState(() => _message = '远程测试已入队');
                  }),
            child: const Text('发送远程测试'),
          ),
          OutlinedButton(
            onPressed: _busy
                ? null
                : () => _run(() async {
                    final result = await (await _initialize()).showLocalTest();
                    if (mounted) {
                      setState(
                        () => _message = result.isEmpty
                            ? '本地通知已提交'
                            : '本地通知受阻：$result',
                      );
                    }
                  }),
            child: const Text('本地通知测试'),
          ),
          OutlinedButton(
            onPressed: _busy
                ? null
                : () => _run(() async {
                    await _sdk?.stop();
                    if (mounted) setState(() => _message = '后台服务已停止，重新开启需重新登记');
                  }),
            child: const Text('停止并撤销绑定'),
          ),
          const SizedBox(height: 16),
          Text(_message),
          const SizedBox(height: 16),
          SelectableText(const JsonEncoder.withIndent('  ').convert(_status)),
          const SizedBox(height: 16),
          const Text('示例仅用于本地联调。后台服务由用户开启，系统强行停止后需回到应用重新开启。'),
        ],
      ),
    ),
  );

  @override
  void dispose() {
    _timer?.cancel();
    _hub.dispose();
    _bootstrap.dispose();
    unawaited(_sdk?.dispose());
    super.dispose();
  }
}
