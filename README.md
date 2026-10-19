# pushx_flutter

Android 自建推送接收 SDK，Flutter 3.44+/Dart 3.12+、Android API 24+。使用官方 Centrifuge Dart 客户端与前台服务、本地通知、SQLite、受保护存储插件。本工程是组合插件的 Flutter package，暂无自行维护的 Kotlin Service；目录名保留用户创建的 `pushx-flutter-plugin`。

配套服务端为 [pushx-go-lib](https://github.com/wfu-work/pushx-go-lib)。宿主负责业务账号、安装登记和通知专用凭据签发；[示例](example/README.md) 提供 Go/Flutter 本地联调步骤。

## 安装与使用

使用工程 `pubspec.yaml`：

```yaml
dependencies:
  pushx_flutter: ^0.1.0
```

运行 `flutter pub get` 后，通过 `import 'package:pushx_flutter/pushx_flutter.dart';` 使用。完整宿主配置见下方 Android 配置与 [示例](example/README.md)；本包只支持 Android 后台推送。

本地开发 SDK 时，可将依赖改为 `pushx_flutter: {path: /absolute/path/to/pushx-flutter-plugin}`。

```dart
final sdk = await Pushx.initialize(
  config: PushxConfig(hub: Uri.parse('https://your-hub.example/api/')),
);
// 宿主用正常账号登录态调用自己的安装登记/签发 API，获得以下数据。
await sdk.activate(
  scope: '$hubIdentity:$accountIdentity',
  subscriptionId: registration.subscriptionId,
  bindingId: registration.bindingId,
  credential: registration.notificationCredential,
  expiresAt: registration.expiresAt,
);
// 用户在可见页面主动开启时执行：
if (await sdk.requestPermission()) await sdk.start();
```

首次初始化不自动申请权限或启动服务。`activate` 只持久配置和受限凭据；启动、停止、重新开启由用户操作协调。显式停止会使绑定本地失效、删除其凭据并尝试远程撤销，重新开启须用新 binding/凭据重新登记。

`stop()` 先在 SQLite 事务中禁用绑定、移除点击事件并持久化清理意图，再分别尝试停服务、撤销、取消通知和删除凭据，各外部步骤有界等待，某一步失败仍继续其他清理。返回 `PushxStopResult` 的 `remoteRevoked/cleanupComplete/errorCodes`，宿主应据此显示结果。通知按最多 8 项一组取消，成功的 ID 逐项提交进度，超时或重启从剩余项继续；已完成的原生停服务、凭据删除步骤和远程撤销凭证会持久化，重复停止不会重置进度。清理失败重开初始化会恢复；重复停止的结果在重启后仍保持。未完成本地清理时不能切换新绑定，旧绑定清理不会停止已启用的新服务。

远程撤销最多等待 3 秒；网络失败或本地凭据缺失都不能证明服务端已撤销。`sdk.store.readPendingRevocations()` 留不含秘密的记录，由宿主以后使用相应账号登录态补撤销，成功后 `completeRevocation(scope,bindingId)`。账号退出的清理钩子须 await stop；不要把完整 refresh token 交给 SDK。

通知点击先落持久队列：读取 `readPendingOpens()`，等登录与路由就绪，重新查询本人业务资源后跳转，再调用 `completeOpen(id)`。`kind=test` 单独进入通知设置。SDK 不执行审批，不把系统通知 submitted 当成用户已阅读。

`snapshot()` 区分期望开启、实际服务、连接、权限与带时间的状态；`status` 是 UI 事件流。`showLocalTest()` 仅测试本地展示，远程测试使用宿主登录态 API。`updatePreferences(sound: ...)` 更新本地配置；服务端业务分类由宿主同步。

UI isolate 内相同参数的并发 `initialize` 共享实例；变更配置前须 await `dispose()`。销毁会等待已接受的 UI 操作、移除回调并关闭 UI 数据库连接，可重复调用；后台开启状态由 `stop()` 控制。为测试/宿主平台适配可注入 store/presenter/platform，实例负责它们直到 dispose 完成。

## Android 配置

宿主负责 Manifest、通知图标与最终服务用途声明。完整可构建配置见 `example/android`：

```xml
<uses-permission android:name="android.permission.INTERNET" />
<uses-permission android:name="android.permission.POST_NOTIFICATIONS" />
<uses-permission android:name="android.permission.FOREGROUND_SERVICE" />
<uses-permission android:name="android.permission.FOREGROUND_SERVICE_SPECIAL_USE" />
<!-- application 内 -->
<service
    android:name="com.pravera.flutter_foreground_task.service.ForegroundService"
    android:exported="false"
    android:stopWithTask="false"
    android:foregroundServiceType="specialUse">
    <property android:name="android.app.PROPERTY_SPECIAL_USE_FGS_SUBTYPE"
        android:value="Describe the actual user-enabled notification connection use case" />
</service>
```

第一版运行器显式使用 specialUse；宿主必须针对实际用途和分发规则确认适用性，不能机械替换成受时长限制的 dataSync。默认不启用开机、升级和系统杀进程后的自动重启，不持有常驻 CPU/Wi-Fi 锁；`allowWakeLock` 可由宿主明确配置，耗电仍需真机验证。

Gradle 开启 Java 17/core-library desugaring，依赖 `com.android.tools:desugar_jdk_libs:2.1.4`；最低 API 24。提供 `drawable/ic_notification` 或通过配置指定自己的白色小图标。生产配置 HTTPS/WSS，默认拒绝 HTTP；示例为 adb 本地联调显式开启 HTTP。

Release 资源压缩可能移除通过字符串引用的通知图标。宿主须提供 `res/raw/pushx_keep.xml`：`<resources xmlns:tools="http://schemas.android.com/tools" tools:keep="@drawable/ic_notification" />`，自定义图标同步修改；示例已包含。

当前将前台服务的间接依赖 `shared_preferences_android` 固定在 `2.4.28`。本次 Release 联调发现 `2.5.0` 的 JNI 协程参数被 R8 改名后会导致初始化 `NoSuchMethodError`；升级该约束前须重新验证带资源压缩与混淆的 Release 包。

`pushx.db` 包含消息和身份范围，应从系统备份/应用设置导出排除；凭据只存系统受保护存储，不能备份恢复后自动启动。示例设置 `allowBackup=false`。本地通知插件在同一 engine 内只应有一个初始化/点击分发负责人，宿主已有通知模块须委托或统一接入，避免覆盖回调。

## 业务 codec 与 API 包装

默认 codec 接受通用 JSON payload。业务工程应实现 `PushxPayloadCodec.validate`，严格验证自身 envelope 白名单、业务类型与外层重复身份字段；这里只允许纯 Dart 数据逻辑。

```dart
@pragma('vm:entry-point')
void myPushxBackground() {
  runPushxBackground(codec: MyBusinessPayloadCodec());
}
// initialize(backgroundEntryPoint: myPushxBackground, ...)
```

后台入口必须为被 Release 保留的顶层函数。后台重建自己的 codec、API decoder 和凭据存储；UI 中构造的闭包、GetX、Dio、数据库对象都不会跨 isolate 生效。若自定义 `PushxCredentialStore`，后台也要用相同的实现，不能只向 UI initialize 注入。自定义 HTTP 包装可在 apiFactory 中构造 `PushxHttpApi(..., decoder: ...)`，并在后台入口同样装配。

## 持久接收规则

- 单 App 一个当前 binding、一个展示运行器；同进程 UI 不再轮询弹同一条远程事件。
- 每 isolate 独立 SQLite 连接；WAL、事务、短忙等待及有界异步重试处理竞争。
- 消息与 received ACK 原子落盘；展示后保存 posted/blocked，ACK HTTP 丢失可重试。
- system ID 稳定并查碰撞；重复 eventKey 复用 ID，展示租约避免两个接收器同时处理。
- 展示后崩溃的记录按同 ID 静音恢复。每个后台 engine 在 SQLite 原子领取独立 runtime owner；偏好配置的 revision 不改变 owner。旧 engine 的 HTTP 401、停止请求、定时任务与迟到展示回调不能禁用新 engine、删除其凭据、取消其通知或停止其原生服务。WebSocket 3501 仅表示连接替换；持久 owner 可重新建立连接，其余旧 engine 只关闭自己的连接。
- ACK 队列按版本确认，旧 received 请求完成不会删掉新 posted 回执。
- 未知版本/坏 payload 在身份可确认时终止为 blocked；过期内容不展示。
- 收件箱上限按 scope/binding 独立计算，默认 5000 条、过期 7 天后清理。`maxInboxItems` 支持 1～50000、`retentionDays` 支持 0～30；旧绑定不会占满当前配额。达到上限不确认新消息，留服务器按 TTL 补发。展示前独立登记最小通知 ID/事件索引，消息正文裁剪不删除这份登记；停止完成取消后才移除，避免通知仍留在 Android 通知栏。
- 网络重连由 Centrifuge 负责；连接后同步、短时恢复和低频核对复用同一去重流程。
- 续期先于其他网络任务；sync/ACK 故障互不阻断。失败按 15 秒到 5 分钟退避并加抖动；一次同步/ACK 分别最多 4 批，避免长期占用队列。每轮从临时分页起点开始，完整同步后 5 分钟核对，未完成时 15 秒继续。
- 定时 tick 和连接状态事件合并，数据队列最多 200 项，过载后由同步补齐；停止绕过队列、中断在途 HTTP 等待。SDK 本地数据库已升级为 v5，保留 v1/v2/v3/v4 数据迁移；v4 的 posted/presenting 记录会补登记通知 ID，未完成清理的撤销凭证也会保留。

## 验证与限制

```sh
flutter pub get
flutter analyze
flutter test
cd example
flutter build apk --release
```

单测使用真实 SQLite FFI 和可控传输/通知边界；示例用于验证真实插件、后台 engine 和 HTTP/WS。`tool/contract_smoke.dart` 可通过 `PUSHX_TEST_URL` 与 `PUSHX_DEMO_TOKEN` 对接本地 Go SDK 示例，验证真实 Go/Dart 收发、ACK、离线同步、续期和撤销。CI 配置包含 analyze/test/示例 Release APK 构建。只支持 Android 后台推送；其他平台的 UI facade 返回 unsupported。

常驻通知不等于系统推送特权。系统强行停止、深 Doze 或厂商后台限制都可能暂停服务，期望开启状态不代表服务实际存活。生产可靠性还依赖宿主的持久 outbox、授权撤销、TTL 和真实机型验收。

本包采用 [MIT License](LICENSE)。底层依赖的版本见 pubspec；Centrifuge、flutter_foreground_task、flutter_local_notifications、sqflite、flutter_secure_storage 等许可证须按各自发布包保留。
