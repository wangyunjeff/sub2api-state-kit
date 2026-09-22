# STATE Kit 独立插件

Sub2API v0.2.7 的开源 OpenAI OAuth transport 插件，提供逐账号 Pro / Team STATE 配置、可选前置代理、动态代理采集、出口 IP 与运行日志、固定业务代理复验、续期和响应异常守护。

请先阅读 [安装与使用说明](../docs/plugin.md)。配置在「插件管理 → STATE Kit → 配置」，基础功能不用修改宿主源码；按分组选择账号、账号名称、IP 管理下拉选择及手动查找 / 代理与模型测试需[v0.3.4 宿主适配](../docs/plugin-host-directory.md)。首次安装需在宿主信任本插件的发布者公钥。

- 插件 ID：`io.github.wangyunjeff.sub2api-state-kit`
- 插件版本：`0.3.4`
- 协议：Sub2API 插件协议 / transport / UI Bridge / HostService v1
- 宿主源码基线：官方 `v0.2.7`，提交 `aea725f2ea644d5592d0bbb1d63b607efa7e200a`
- 当前部署范围：单应用实例
- 默认：所有 STATE 开关关闭，不含任何真实账号或代理配置
- 可选：在配置页开启“强制使用 Astra”，将插件账号的上游模型统一改为 `gpt-6-astra`

## 开发

```bash
go test -race ./...
node --test ui-tests/*.test.cjs
go build -trimpath -o build/state-kit ./cmd/state-kit
```

`internal/pluginapi/v1` 基于上述官方源码的公开契约，追加了可选的 ListResources / ResolveProxy 及 RunAction RPC，保留源码和许可归属。其余实现为本项目独立实现，不包含官方闭源传输插件。许可证沿用本仓库 LGPL-3.0。

发布者签名公钥在 `release/`；签名私钥必须在仓库外。参见 `../scripts/package_plugin.py` 和 `integration/` 中的宿主安装/运行集成测试。


v0.3.3 新增单账号开关独立保存、立即重新获取票据、管理员手动提示词测试、出口 IP 检测和隔离 HTML 预览。无票据放行默认开启（旧配置未指定此字段时也开启）；设为 `allow_without_ticket: false` 可恢复无票据返回暂不可用的行为。后台获取及守护继续运行。
手动动作需要更新宿主适配（RunAction RPC、管理员动作路由和 UI Bridge），旧版宿主保留基础转发和配置功能。

v0.3.4 在新版宿主适配下支持先选分组、再选账号；插件未启动时也能加载目录。新增账号默认关闭，需先保存设置再开启。旧宿主保留手动 ID 添加。
