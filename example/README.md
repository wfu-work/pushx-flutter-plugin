# Pushx Android 示例

此示例仅以 `path: ../` 依赖 SDK，不依赖 Codexc。用于验证 Go 网关、通知专用凭据、Android 前台服务、系统通知与 ACK。

## 运行

先在相邻的 `pushx-go-lib` 目录启动开发服务：

```sh
export PUSHX_DEMO_TOKEN='replace-with-your-local-demo-token'
go run ./examples/server
```

连接 Android 设备后：

```sh
adb -s <serial> reverse tcp:8099 tcp:8099
flutter pub get
flutter run --release -d <serial>
```

在界面输入 `http://127.0.0.1:8099/` 与上述开发口令，点击“登记并开启后台推送”。同意通知权限后点击“发送远程测试”，检查系统通知。也可把应用退到桌面，从电脑发送：

```sh
curl -X POST -H "Authorization: Bearer $PUSHX_DEMO_TOKEN" \
  http://127.0.0.1:8099/demo/send
curl -H "Authorization: Bearer $PUSHX_DEMO_TOKEN" \
  http://127.0.0.1:8099/demo/state
```

状态应从排队变为 `posted`；它表示本地通知已提交给系统，不代表用户阅读。点击通知后示例显示消息 ID，再完成持久点击队列。点击“停止并撤销绑定”或常驻通知中的“停止”，须重新登记后才能再次启动。

“本地通知测试”只检查 Android 展示链路，不会伪造服务端投递回执。

## Android 配置

`android/app` 已包含通知图标及 Release 资源保留、Java 17/desugaring、API 24 最低版本、specialUse 前台服务与通知权限。生产工程须复制对应配置并按实际用途完成分发声明，详见 SDK README。

开发口令仅用于示例登记/发送；连接与 ACK 使用服务端另行签发的受限凭据，保存在系统受保护存储。本地 SQLite 不保存明文凭据。

示例默认允许本地 HTTP；正式工程使用 HTTPS/WSS。Go 示例只在内存保存队列与凭据，重启后需要重新登记。示例 Release 使用 debug key 签名，不用于发布。

验证结束后可移除端口转发：

```sh
adb -s <serial> reverse --remove tcp:8099
```

已执行的检查与未覆盖的系统场景见根目录 `docs/verification.md`。
