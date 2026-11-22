# -*- coding: utf-8 -*-
"""
hunyuan-review-mcp —— 把混元包装成 MCP server，挂给豆包当评审员。

链路（闭合在豆包内部，不需要外部编排脚本）：
  豆包产出 → 调 collect_evidence 跑机器闸门 → 调 review 交混元评审
          → high 级问题逐条打回 → 豆包改 → 再调 review（只审增量）
          → 2 轮封顶，交人裁决

为什么中间必须隔一层网关，而不是豆包直连混元：
  1. 轮次、增量、封顶这些状态要有人记，模型自己记不住
  2. 证据必须先落盘再送审，否则「编译通过」是评审员编出来的
  3. 混元默认 5 并发且主子账号共享，重试与退避得统一收口

协议：streamableHttp（POST /mcp），另留 /sse + /message 给老客户端。
依赖：pip install fastapi uvicorn openai
环境变量：
  HUNYUAN_API_KEY   必填，腾讯云混元控制台 → API Key 管理
  HUNYUAN_MODEL     可选，默认 hunyuan-t1-latest
  PROJECT_ROOT      可选，默认当前目录；代码类评审请指到仓库根
  GIT_BASE          可选，默认 main，增量 diff 的参照分支
  MAX_ROUND         可选，默认 2
  PORT              可选，默认 8080
  BIND              可选，默认 127.0.0.1 —— 改 0.0.0.0 前先读文末「安全」一节
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from openai import OpenAI

import evidence
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass
# ---------------------------------------------------------------- 配置
# 密钥与模型从同级 .env 读，代码里不留任何凭据。
# 优先级：真实环境变量 > .env 文件 > 代码默认值。所以临时换模型仍可
# `HUNYUAN_MODEL=hunyuan-lite python server.py`，不必改文件。

def _load_dotenv(path: str) -> None:
    """极简 .env 解析：支持 # 注释、export 前缀、单双引号；不覆盖已有环境变量。"""
    if not os.path.isfile(path):
        return
    for raw in open(path, encoding="utf-8"):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip()
        if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
            v = v[1:-1]
        if k and k not in os.environ:      # 真实环境变量优先
            os.environ[k] = v

_load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

HY_KEY = os.environ.get("HUNYUAN_API_KEY", "")
HY_MODEL = os.getenv("HUNYUAN_MODEL", "hy4")   # 深度评审默认；快审可换 hunyuan-turbos-latest
PROJECT_ROOT = os.path.abspath(os.getenv("PROJECT_ROOT", os.getcwd()))
GIT_BASE = os.getenv("GIT_BASE", "main")
MAX_ROUND = int(os.getenv("MAX_ROUND", "2"))
REVIEW_TIMEOUT = int(os.getenv("REVIEW_TIMEOUT", "600"))
HY_RETRY = int(os.getenv("HY_RETRY", "4"))

CHECKS_PATH = os.getenv("CHECKS_PATH", os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "CHECKS.md"))

HY_BASE_URL = os.getenv("HUNYUAN_BASE_URL", "https://tokenhub.tencentmaas.com/v1")
hy = OpenAI(api_key=HY_KEY,
            base_url=HY_BASE_URL) if HY_KEY else None

PROMPT_VERSION = "3"

# ---------------------------------------------------------------- 评审员人设

SYS_REVIEWER = """你是只读评审员。你不能修改任何文件，不能替作者重写，不能执行任何命令。
你的全部输入是人给的：判据 + 机器采集的证据 + 待审产出。证据里没有的，一律视为「未证实」。

# 判定方式
把判据逐条判成 PASS / FAIL / 未证实，一条都不许跳过、不许合并。
默认结论是 REJECTED。要判 approved，每条 PASS 都必须附证据定位
（文件:行号 / 命令输出片段 / 证据块里的条目名）。

# 只审这五类问题
1. 事实错误 —— 与证据或可查事实冲突
2. 逻辑跳跃 —— 前提推不出结论，缺中间步骤
3. 遗漏的边界 —— 空输入、溢出、并发、失败路径、回滚
4. 证据缺失 —— 声称「已验证 / 已测试 / 已跑通」却没有任何证据支撑
5. 判据未满足 —— CHECKS 里写死的条目被绕过

# 不要做这些
- 不评价文风，不说「整体不错」「建议进一步优化」这类无信息量的话
- 不因为「看起来很专业」就放行
- 不替作者想借口；拿不准就写进 questions_for_author
- 不复述待审产出的内容

# 硬性闸门优先
若证据块里出现「硬性闸门未过」（编译失败 / 测试失败 / sanitizer 报错），
verdict 必须是 changes_requested，且这些问题的 severity 必须是 high。
此时不必再细抠风格与命名——先把红灯灭掉。

# 输出
最后一行输出严格 JSON，不要包裹代码块，不要在 JSON 后再写任何字：
{"verdict":"approved|changes_requested",
 "rubric":[{"item":"判据原文","result":"PASS|FAIL|UNVERIFIED","evidence":"定位或空"}],
 "findings":[{"severity":"high|med|low","location":"文件:行号或章节",
              "issue":"问题是什么，一句话","recommendation":"具体怎么改，一句话"}],
 "questions_for_author":["需要作者澄清的策略问题，没有就空数组"]}

severity 口径：high = 会导致错误结果 / 编译或测试不过 / 证据造假；
med = 设计缺陷但不致命；low = 可读性与一致性。"""

SYS_REVIEWER_DOC = """你是只读评审员，审的是文档 / 方案 / 报告，不是代码。
你不能修改任何内容，不能替作者重写。证据里没有的，一律视为「未证实」。

# 判定方式
判据逐条判 PASS / FAIL / 未证实，一条不跳。默认结论 REJECTED。
要判 approved，每条 PASS 必须附定位（章节 / 段落 / 来源链接 / 证据条目名）。

# 只审这五类
1. 事实错误 —— 数字、日期、API、版本、引用与来源冲突
2. 逻辑跳跃 —— 前提推不出结论
3. 遗漏的边界 —— 方案没写的失败路径、回滚、成本、权限
4. 证据缺失 —— 每个数字后面有没有来源；没有就标出来
5. 判据未满足 —— CHECKS 写死的条目被绕过

# 不要做
不评价文风，不夸「结构清晰」，不复述原文，不替作者圆场。
拿不准的事实写进 questions_for_author，不要猜。

# 输出
最后一行输出严格 JSON，不包裹代码块，JSON 后不写任何字：
{"verdict":"approved|changes_requested",
 "rubric":[{"item":"判据原文","result":"PASS|FAIL|UNVERIFIED","evidence":"定位或空"}],
 "findings":[{"severity":"high|med|low","location":"章节或段落",
              "issue":"一句话","recommendation":"一句话"}],
 "questions_for_author":[]}

severity 口径：high = 事实错误 / 结论无依据 / 关键风险漏写；med = 论证不完整；low = 表述歧义。"""

# ---------------------------------------------------------------- 状态

class Session:
    def __init__(self, sid: str):
        self.sid = sid
        self.round = 0
        self.hist: list[str] = []
        self.last_evidence: str = ""
        self.created = time.time()

    def to_dict(self) -> dict:
        return {"session_id": self.sid, "round": self.round,
                "evidence_cached": bool(self.last_evidence),
                "created": round(self.created, 1)}


SESS: dict[str, Session] = {}
LOCK = threading.RLock()          # RLock：collect_evidence 内部会再取一次
POOL = ThreadPoolExecutor(max_workers=2)   # 证据采集是 CPU/IO 密集，别塞进事件循环

def get_session(sid: str | None) -> Session:
    sid = sid or "default"
    with LOCK:
        return SESS.setdefault(sid, Session(sid))

# ---------------------------------------------------------------- 混元调用

class HYError(RuntimeError):
    pass


def call_hunyuan(system: str, user: str, timeout: int = REVIEW_TIMEOUT) -> str:
    """混元默认 5 并发且主子账号共享，退避重试不是可选项。"""
    if hy is None:
        raise HYError("HUNYUAN_API_KEY 未配置。腾讯云控制台 → 混元大模型 → API Key 管理")
    last = None
    for i in range(HY_RETRY):
        try:
            r = hy.chat.completions.create(
                model=HY_MODEL, temperature=0.2, timeout=timeout,
                max_tokens=int(os.getenv("HY_MAX_TOKENS", "16000")),
                messages=[{"role": "system", "content": system},
                          {"role": "user", "content": user}],
            )
            return (r.choices[0].message.content or "").strip()
        except Exception as e:                                # noqa: BLE001
            last = e
            if i == HY_RETRY - 1:
                break
            time.sleep(min(2 ** i * 1.5, 20))
    raise HYError(f"混元调用失败（重试 {HY_RETRY} 次）：{type(last).__name__}: {last}")


def extract_json(txt: str) -> dict:
    """
    取最后一个平衡的 JSON 对象。不用 stop 参数——混元的 stop 语义与 OpenAI
    相反（停在匹配内容之后），靠它截 JSON 会拿到被截断的串。
    """
    idxs = [m.start() for m in re.finditer(r"\{", txt)]
    best = None
    for a in idxs:
        d = 0
        for i, c in enumerate(txt[a:], a):
            d += (c == "{") - (c == "}")
            if d == 0:
                try:
                    cand = json.loads(txt[a:i + 1])
                except json.JSONDecodeError:
                    break
                if isinstance(cand, dict) and "verdict" in cand:
                    best = cand
                break
    if best is None:
        raise HYError("评审输出里找不到合法 JSON（需含 verdict 字段）")
    return best


# ---------------------------------------------------------------- 工具实现

def load_checks(kind: str) -> str:
    try:
        txt = open(CHECKS_PATH, encoding="utf-8").read()
    except OSError as e:
        return f"（判据文件读不到：{e}；请用通用口径：事实可溯源、逻辑无跳跃、边界无遗漏、断言有证据）"
    if kind == "code":
        m = re.search(r"<!-- CODE:start -->(.*?)<!-- CODE:end -->", txt, re.S)
        return (m.group(1).strip() if m else txt)
    m = re.search(r"<!-- DOC:start -->(.*?)<!-- DOC:end -->", txt, re.S)
    return (m.group(1).strip() if m else txt)


def tool_collect_evidence(args: dict) -> dict:
    root = os.path.abspath(args.get("project_root") or PROJECT_ROOT)
    base = args.get("base") or GIT_BASE
    profiles = args.get("profiles") or ["plain"]
    profiles = [p for p in profiles if p in ("plain", "asan", "tsan")] or ["plain"]
    run_tests = bool(args.get("run_tests", True))
    do_tidy = bool(args.get("tidy", True))

    if not os.path.isdir(root):
        return _text(f"项目路径不存在：{root}", is_error=True)

    res = evidence.collect(root=root, base=base, profiles=profiles,
                           run_tests=run_tests, do_tidy=do_tidy)
    block = evidence.to_review_block(res)

    sid = args.get("session_id")
    if sid:
        with LOCK:
            get_session(sid).last_evidence = block

    head = (f"证据采集完成，用时 {res.get('elapsed')}s。\n"
            f"硬性闸门：{'未过 → ' + ', '.join(res.get('hard_fail', [])) if res.get('hard_fail') else '全部通过'}\n"
            f"改动文件 {len(res.get('changed', []))} 个。\n\n"
            f"**接下来必须调用 review 工具**，把待审内容通过 content 传入；"
            f"证据已按 session 缓存，review 会自动带上，不用你复述。\n\n")
    return _text(head + block,
                 structured={"project": res.get("project"),
                             "gates": res.get("gates"),
                             "hard_fail": res.get("hard_fail"),
                             "changed": res.get("changed", [])[:80],
                             "elapsed": res.get("elapsed")})


def tool_review(args: dict) -> dict:
    content = (args.get("content") or "").strip()
    if not content:
        return _text("content 为空，没有可审的内容。", is_error=True)
    kind = args.get("kind") or ("code" if args.get("project_root") or
                                args.get("session_id") else "doc")
    if kind not in ("code", "doc"):
        kind = "doc"

    s = get_session(args.get("session_id"))
    with LOCK:
        s.round += 1
        rnd = s.round
        prev = s.hist[-1] if s.hist else None
        ev = s.last_evidence
        s.last_evidence = ""          # 证据只随第一轮走，后续只审增量

    checks = load_checks(kind)
    sys_p = SYS_REVIEWER if kind == "code" else SYS_REVIEWER_DOC

    parts = [f"# 判据（{kind}）\n{checks}"]
    if ev:
        parts.append(ev)
    parts.append(f"# 待审产出（第 {rnd} 版）\n{content}")
    if prev:
        parts.append(f"# 上一轮评审结论（本轮只审增量，已修好的不要重提）\n{prev}")
    parts.append(f"# 元信息\n评审轮次：第 {rnd} 轮 / 上限 {MAX_ROUND} 轮。"
                 f"这是{'最后一轮' if rnd >= MAX_ROUND else '可打回轮'}。"
                 f"评审员版本 {PROMPT_VERSION}，模型 {HY_MODEL}。")
    user = "\n\n".join(parts)

    try:
        raw = call_hunyuan(sys_p, user)
        v = extract_json(raw)
    except HYError as e:
        with LOCK:
            s.round -= 1              # 失败不算一轮
        return _text(f"评审没跑成：{e}\n\n**不要自行判定通过**，修好配置后重试同一 session。",
                     is_error=True)

    with LOCK:
        s.hist.append(json.dumps(v, ensure_ascii=False))

    return _text(_render(v, rnd), structured={
        "verdict": v.get("verdict"), "round": rnd,
        "high": _count_high(v), "exhausted": rnd > MAX_ROUND,
        "findings": v.get("findings", [])[:40],
        "questions_for_author": v.get("questions_for_author", [])[:10]})


def tool_get_findings(args: dict) -> dict:
    s = get_session(args.get("session_id"))
    with LOCK:
        h = list(s.hist)
    if not h:
        return _text(f"会话 {s.sid} 还没有评审记录。先调 collect_evidence（代码类）再调 review。")
    body = "\n\n".join(f"### 第 {i+1} 轮\n```json\n{x}\n```" for i, x in enumerate(h))
    return _text(f"会话 {s.sid} 共 {len(h)} 轮评审：\n\n{body}")


def tool_reset(args: dict) -> dict:
    sid = args.get("session_id") or "default"
    with LOCK:
        SESS.pop(sid, None)
    return _text(f"会话 {sid} 已清空，轮次归零。")


def tool_whoami(_: dict) -> dict:
    ok = False
    detail = "HUNYUAN_API_KEY 未配置"
    if hy is not None:
        try:
            t0 = time.time()
            call_hunyuan("回复两个字：就绪", "ping", timeout=60)
            ok = True
            detail = f"混元连通正常（{round(time.time()-t0,1)}s）"
        except HYError as e:
            detail = f"混元连通失败：{e}"
    return _text(
        f"评审员：**混元 {HY_MODEL}**（OpenAI 兼容端点）\n"
        f"连通性：{detail}\n"
        f"项目根：{PROJECT_ROOT}\n基准分支：{GIT_BASE}\n"
        f"轮次上限：{MAX_ROUND}\n判据文件：{CHECKS_PATH}\n"
        f"采集器：cmake / clang-tidy / clang-format / ctest / ASan+UBSan / TSan / nm\n"
        f"提示：证据采集是只读的，不会改你的文件，也不会 commit。",
        structured={"ready": ok, "model": HY_MODEL, "project_root": PROJECT_ROOT,
                    "git_base": GIT_BASE, "max_round": MAX_ROUND})


# ---------------------------------------------------------------- 渲染

def _count_high(v: dict) -> int:
    try:
        return sum(1 for f in v.get("findings", [])
                   if str(f.get("severity", "")).lower() == "high")
    except (AttributeError, TypeError):
        return 0


def _render(v: dict, rnd: int) -> str:
    verdict = str(v.get("verdict", "changes_requested")).lower()
    findings = v.get("findings") or []
    high = [f for f in findings if str(f.get("severity", "")).lower() == "high"]
    med = [f for f in findings if str(f.get("severity", "")).lower() == "med"]
    low = [f for f in findings if str(f.get("severity", "")).lower() == "low"]
    icon = {"approved": "✅", "changes_requested": "🔁"}.get(verdict, "❓")

    out = [f"## {icon} 评审第 {rnd} 轮 — {verdict}",
           f"high {len(high)} / med {len(med)} / low {len(low)}", ""]

    rub = v.get("rubric") or []
    if rub:
        bad = [r for r in rub if str(r.get("result", "")).upper() in ("FAIL", "UNVERIFIED")]
        out.append(f"### 判据逐条（{len(rub) - len(bad)}/{len(rub)} 通过）")
        for r in rub:
            mark = {"PASS": "✓", "FAIL": "✗", "UNVERIFIED": "?"}.get(
                str(r.get("result", "")).upper(), "-")
            out.append(f"- {mark} {r.get('item','')}"
                       + (f"　→　{r.get('evidence')}" if r.get("evidence") else ""))
        out.append("")

    for title, group in (("🔴 high（必须改）", high),
                         ("🟡 med（建议改）", med),
                         ("⚪ low（可选）", low)):
        if not group:
            continue
        out.append(f"### {title}")
        for i, f in enumerate(group, 1):
            out.append(f"{i}. **{f.get('location','未定位')}** — {f.get('issue','')}")
            if f.get("recommendation"):
                out.append(f"   改法：{f['recommendation']}")
        out.append("")

    q = v.get("questions_for_author") or []
    if q:
        out.append("### ❓ 需要你拍板的策略问题（不要自行脑补答案）")
        out += [f"- {x}" for x in q]
        out.append("")

    if high and rnd > MAX_ROUND:
        out.append(f"### ⚠️ 已到 {MAX_ROUND} 轮上限，停止自改")
        out.append("把上面的策略问题连同上文一起交给人裁决，不要再调 review——"
                   "第 3 轮之后只是在加噪声和烧钱。")
    elif high:
        out.append(f"### 下一步")
        out.append(f"逐条改掉上面 {len(high)} 条 high，改完**带同一个 session_id 再调一次 review**。"
                   f"不要解释、不要辩护，只交付修改后的完整内容。")
        out.append("改完如果想让机器先过一遍闸门，再调一次 collect_evidence。")
    else:
        out.append("### ✅ 可以交付")
        out.append("交人之前最后确认一件事：证据里「SKIP」的项是不是真的不适用，"
                   "而不是采集器没跑起来。")

    return "\n".join(out)


def _text(t: str, is_error: bool = False,
          structured: dict | None = None) -> dict:
    d: dict[str, Any] = {"content": [{"type": "text", "text": t}],
                         "isError": is_error}
    if structured is not None:
        d["structuredContent"] = structured
    return d


# ---------------------------------------------------------------- MCP 协议

TOOLS: list[dict] = [
    {"name": "collect_evidence",
     "description":
         "【代码类任务，审前必调】跑机器闸门并采集证据：CMake 配置、编译（-Werror）、"
         "clang-tidy 增量、clang-format、ctest、ASan+UBSan、TSan、ABI 符号清单、"
         "头文件改动自动展开到引用它的翻译单元。采集器只读，不改文件不 commit。"
         "结果按 session 缓存，随后的 review 会自动带上。",
     "inputSchema": {"type": "object", "properties": {
         "project_root": {"type": "string",
                          "description": "仓库根目录绝对路径，缺省用服务端 PROJECT_ROOT"},
         "base": {"type": "string",
                  "description": "增量 diff 的参照分支，缺省 main"},
         "profiles": {"type": "array", "items": {"type": "string"},
                      "description": "编译闸门组合，可多选：plain / asan / tsan。缺省 [\"plain\"]"},
         "run_tests": {"type": "boolean", "description": "是否跑测试，缺省 true"},
         "tidy": {"type": "boolean", "description": "是否跑 clang-tidy，缺省 true"},
         "session_id": {"type": "string",
                        "description": "同一任务全程传同一个值，用于缓存证据与只审增量"}},
         "required": []}},
    {"name": "review",
     "description":
         "把产出交混元评审，返回逐条判据结果 + 分级问题 + 改法。有 high 级问题就必须改完再调一次"
         "（带同一 session_id，本轮只审增量）。超过 2 轮网关会强制停下并把策略问题交给人。",
     "inputSchema": {"type": "object", "properties": {
         "content": {"type": "string",
                     "description": "待审的完整产出（代码 diff 或文档正文）"},
         "kind": {"type": "string", "enum": ["code", "doc"],
                  "description": "评审口径：code 走 C++ 判据，doc 走文档判据。缺省按有无 project_root 推断"},
         "session_id": {"type": "string",
                        "description": "同一任务多次评审传同一个值，用于只审增量与轮次计数"},
         "project_root": {"type": "string",
                          "description": "代码类任务填仓库根，用于推断 kind=code"}},
         "required": ["content"]}},
    {"name": "get_findings",
     "description": "读本会话全部轮次的评审原始 JSON，用于自己复盘或交给用户看。",
     "inputSchema": {"type": "object", "properties": {
         "session_id": {"type": "string"}}, "required": []}},
    {"name": "reset_session",
     "description": "清空会话的轮次与历史，从头开始一轮新的评审。",
     "inputSchema": {"type": "object", "properties": {
         "session_id": {"type": "string"}}, "required": []}},
    {"name": "whoami",
     "description": "查评审员身份：模型、连通性、项目根、基准分支、轮次上限、判据文件路径。先用它自检环境。",
     "inputSchema": {"type": "object", "properties": {}, "required": []}},
]

# 网关层强制：支持该字段的客户端（Claude Code / Codex / Cline / Cursor 等）会据此
# 要求模型必须调用；不认的客户端忽略它，靠 system 提示里的约定兜底。
REQUIRED_TOOLS = ["collect_evidence", "review"]

HANDLERS = {
    "collect_evidence": tool_collect_evidence,
    "review": tool_review,
    "get_findings": tool_get_findings,
    "reset_session": tool_reset,
    "whoami": tool_whoami,
}

RESOURCES = [
    {"uri": "checks://code", "name": "C++ 评审判据",
     "description": "逐条可判 PASS/FAIL 的 C++ 判据，评审以它为准；改这里的文字就等于改评审标准",
     "mimeType": "text/markdown"},
    {"uri": "checks://doc", "name": "文档 / 方案评审判据",
     "description": "文档与方案的判据，重点是数字可溯源与风险无遗漏",
     "mimeType": "text/markdown"},
]

# ---------------------------------------------------------------- ASGI 应用

app = FastAPI(title="hunyuan-review-mcp", version="1.0")


def _ok(i: Any, result: Any) -> JSONResponse:
    return JSONResponse({"jsonrpc": "2.0", "id": i, "result": result})


def _err(i: Any, code: int, msg: str) -> JSONResponse:
    return JSONResponse({"jsonrpc": "2.0", "id": i,
                         "error": {"code": code, "message": msg}})


@app.get("/health")
async def health():
    return {"ok": True, "model": HY_MODEL, "key_configured": bool(HY_KEY),
            "project_root": PROJECT_ROOT, "sessions": len(SESS)}


@app.get("/sse")
async def sse(request: Request):
    """老客户端的 SSE 传输。endpoint 事件告诉它往哪 POST。"""
    async def gen():
        yield "event: endpoint\ndata: /message\n\n"
        while not await request.is_disconnected():
            yield ": ping\n\n"
            await asyncio.sleep(15)
    return StreamingResponse(gen(), media_type="text/event-stream")


@app.post("/message")
async def sse_message(request: Request):
    """SSE 模式下的请求入口：收到就派发，结果只回给 POST 的响应（简化实现）。"""
    body = await request.json()
    r = await dispatch(body)
    return r if isinstance(r, JSONResponse) else JSONResponse({"ok": True})


@app.post("/mcp")
async def mcp(request: Request):
    body = await request.json()
    return await dispatch(body)


async def dispatch(body: dict) -> JSONResponse:
    m = body.get("method")
    i = body.get("id")
    notify = i is None

    if m == "initialize":
        return _ok(i, {
            "protocolVersion": body.get("protocolVersion", "2025-03-26"),
            "capabilities": {"tools": {"listChanged": False},
                             "resources": {"subscribe": False, "listChanged": False}},
            "serverInfo": {"name": "hunyuan-reviewer", "version": "1.0"},
            "instructions": INSTRUCTIONS,
        })
    if m == "notifications/initialized":
        return JSONResponse({}, status_code=202)
    if m == "ping":
        return _ok(i, {})

    if m == "tools/list":
        return _ok(i, {"tools": TOOLS, "requiredTools": REQUIRED_TOOLS})

    if m == "tools/call":
        p = body.get("params") or {}
        name = p.get("name")
        fn = HANDLERS.get(name)
        if fn is None:
            return _err(i, -32602, f"未知工具：{name}。可用：{', '.join(HANDLERS)}")
        args = p.get("arguments") or {}
        # 证据采集是长任务，丢线程池，别堵事件循环
        if name == "collect_evidence":
            loop = asyncio.get_running_loop()
            try:
                res = await asyncio.wait_for(
                    loop.run_in_executor(POOL, fn, args), timeout=3600)
            except asyncio.TimeoutError:
                return _err(i, -32000, "证据采集超过 3600s，已终止。建议先只跑 profiles=[\"plain\"]")
        else:
            res = await asyncio.get_running_loop().run_in_executor(POOL, fn, args)
        return _ok(i, res)

    if m == "resources/list":
        return _ok(i, {"resources": RESOURCES})

    if m == "resources/read":
        uri = (body.get("params") or {}).get("uri")
        kind = "code" if uri == "checks://code" else "doc"
        return _ok(i, {"contents": [{"uri": uri, "mimeType": "text/markdown",
                                     "text": load_checks(kind)}]})

    if notify:
        return JSONResponse({}, status_code=202)
    return _err(i, -32601, f"不支持的方法：{m}")


INSTRUCTIONS = """这是混元驱动的只读评审员。工作铁律：

1. 产出任何实质性内容后，必须先调 collect_evidence（代码类）再调 review，
   拿到 approved 才算完成。不许自己给自己盖章。
2. 代码类任务一律先 collect_evidence：没有机器证据，评审员只能猜，
   「编译通过」「测试全绿」这类断言不许自己写。
3. review 返回 high 级问题时，逐条改完，带同一个 session_id 再调 review；
   只审增量，不要重做整篇。
4. 最多 2 轮。第 3 轮网关会拒绝继续，此时把 questions_for_author
   原样交给人，不要自行拍板。
5. 评审意见里是 med/low 级问题、且你判断属于风格偏好的，可以保留自己的写法，
   但要在交付说明里写明「评审提出 X，我保留，理由是 Y」。
"""


if __name__ == "__main__":
    _env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not HY_KEY:
        print(f"⚠️  HUNYUAN_API_KEY 未设置，review 工具会直接报错，证据采集仍可用。")
        print(f"    在 {_env_file} 里写一行：HUNYUAN_API_KEY=你的key")
    elif os.path.isfile(_env_file):
        print(f"✓ 已读取 {_env_file}")
    print(f"评审模型：{HY_MODEL}\n项目根：{PROJECT_ROOT}\n基准分支：{GIT_BASE}")
    uvicorn.run(app, host=os.getenv("BIND", "127.0.0.1"),
                port=int(os.getenv("PORT", "8080")), log_level="info")
