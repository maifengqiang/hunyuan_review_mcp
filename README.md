# hunyuan-review-mcp

把混元包装成 MCP server，挂给豆包当评审员：豆包产出 → 机器闸门采证 → 混元评审
→ high 级问题打回 → 豆包改 → 只审增量，2 轮封顶交人。链路闭合在豆包内部。

## 安装

    pip install fastapi uvicorn openai
    cp .env.example .env      # 填 HUNYUAN_API_KEY / HUNYUAN_MODEL / PROJECT_ROOT
    python server.py          # 看到「✓ 已读取 .env」即生效

优先级：真实环境变量 > .env > 代码默认值。临时换模型可直接
`HUNYUAN_MODEL=hunyuan-lite python server.py`。

## 挂进豆包

设置 → MCP → 添加服务：

    {"mcpServers":{"hunyuan-reviewer":{"type":"streamableHttp",
     "url":"http://127.0.0.1:8080/mcp"}}}

若界面只有 command 栏，填 `npx -y mcp-remote http://127.0.0.1:8080/mcp`。
配完先让豆包调一次 `whoami` 自检连通性。

## 工具

| 工具 | 作用 |
|---|---|
| whoami | 查模型、连通性、项目根、基准分支、轮次上限 |
| collect_evidence | 跑机器闸门并采证（只读，不改文件不 commit） |
| review | 交混元评审，返回逐条判据 + 分级问题 + 改法 |
| get_findings | 读本会话全部轮次的原始 JSON |
| reset_session | 轮次归零，重开一轮 |

collect_evidence 采集：cmake 配置、编译(-Werror)、clang-tidy 增量、
clang-format、ctest、ASan+UBSan、TSan、nm 符号清单、diff stat；
头文件改动自动展开到引用它的翻译单元。

## 改判据

编辑 CHECKS.md。`<!-- CODE:start/end -->` 是代码段，`<!-- DOC:start/end -->`
是文档段。改文字即改评审标准，比换模型管用。

## 安全

collect_evidence 能以你的身份跑 cmake 与编译器，BIND 保持 127.0.0.1。
远程使用走 SSH 隧道或反向代理鉴权，不要直接暴露端口。
.env 已在 .gitignore 中，密钥不要提交。
