# -*- coding: utf-8 -*-
"""
hunyuan-review-mcp —— 把混元包装成 MCP server，挂给豆包当评审员。

链路（闭合在豆包内部，不需要外部编排脚本）：
  豆包产出 -> collect_evidence 跑机器闸门 -> review 交混元评审
          -> high 级问题逐条打回 -> 豆包改 -> 再调 review（只审增量）
          -> MAX_ROUND 封顶，交人裁决

为什么中间必须隔一层网关，而不是豆包直连混元：
  1. 轮次、增量、封顶这些状态要有人记，模型自己记不住
  2. 证据必须先落盘再送审，否则「编译通过」是评审员编出来的
  3. 混元并发有限且主子账号共享，重试与退避得统一收口

协议：streamableHttp（POST /mcp），另留 /sse + /message 给老客户端。
依赖：pip install fastapi uvicorn openai
环境变量（也可写同级 .env，真实环境变量优先）：
  HUNYUAN_API_KEY   必填
  HUNYUAN_BASE_URL  缺省 https://tokenhub.tencentmaas.com/v1
  HUNYUAN_MODEL     缺省 hy4
  PROJECT_ROOT      代码类评审请指到仓库根
  GIT_BASE          缺省 main
  MAX_ROUND         缺省 2
  PORT / BIND       缺省 127.0.0.1:8080，改 0.0.0.0 前先读 README 安全节
  MCP_PLAIN_TEXT    缺省 1。=1 时 tools/call 只回纯文本 content，
                    不带 structuredContent —— 客户端 schema 校验最稳，
                    豆包报 invalid raw MCP result 时保持这个值
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

# ---- 编码三连：Windows 控制台 + 重定向日志都走 UTF-8，根治乱码与 UnicodeEncodeError
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from openai import OpenAI

import evidence


# ---------------------------------------------------------------- 配置

def _load_dotenv(path: str) -> None:
    """极简 .env 解析：支持 # 注释、export 前缀、单双引号；不覆盖已有环境变量。"""
    if not os.path.isfile(path):
        return
    for raw in open(path, encoding="utf-8-sig"):   # utf-8-sig：顺手吃掉 BOM
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
        if k and k not in os.environ:
            os.environ[k] = v


_HERE = os.path.dirname(os.path.abspath(__file__))
_load_dotenv(os.path.join(_HERE, ".env"))

HY_KEY = os.environ.get("HUNYUAN_API_KEY", "")
HY_MODEL = os.getenv("HUNYUAN_MODEL", "hy4")
HY_BASE_URL = os.getenv("HUNYUAN_BASE_URL", "https://tokenhub.tencentmaas.com/v1")
PROJECT_ROOT = os.path.abspath(os.getenv("PROJECT_ROOT", os.getcwd()))
GIT_BASE = os.getenv("GIT_BASE", "main")
MAX_ROUND = int(os.getenv("MAX_ROUND", "2"))
REVIEW_TIMEOUT = int(os.getenv("REVIEW_TIMEOUT", "600"))
HY_RETRY = int(os.getenv("HY_RETRY", "4"))
HY_MAX_TOKENS = int(os.getenv("HY_MAX_TOKENS", "16000"))

# 关键开关：默认关闭 structuredContent。豆包的 invalid raw MCP result
# 就是它对带 structuredContent 的返回做 schema 校验失败导致的。
PLAIN_TEXT = os.getenv("MCP_PLAIN_TEXT", "1") not in ("0", "false", "False", "")

# 单次工具返回的安全体积。超了就裁，宁可少给也不让客户端解析失败。
MAX_TEXT_BYTES = int(os.getenv("MCP_MAX_TEXT_BYTES", "48000"))

CHECKS_PATH = os.getenv("CHECKS_PATH", os.path.join(_HERE, "CHECKS.md"))

hy = OpenAI(api_key=HY_KEY, base_url=HY_BASE_URL) if HY_KEY else None

PROMPT_VERSION = "4"

# 协议版本：按客户端声明的来，但只认它和我们都能接受的最高值。
# 原先硬编码 2025-03-26，遇到只认 2024-11-05 的老客户端会握手失败。
SUPPORTED_PROTOCOL = ("2025-06-18", "2025-03-26", "2024-11-05")

# ---------------------------------------------------------------- 评审员人设

SYS_REVIEWER = """你是只读评审员。你不能修改任何文件，不能替作者重写，不能执行任何命令。
你的全部输入是人给的：判据 + 机器采集的证据 + 待审产出。证据里没有的，一律视为「未证实」。

# 判定方式
把判据逐条判成 PASS / FAIL / 未证实，一条都不许跳过、不许合并。
默认结论是 changes_requested。要判 approved，每条 PASS 都必须附证据定位
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

# 输出纪律（很重要）
只允许输出一个 JSON 对象，不要 markdown 代码块，不要任何前后缀说明文字。
字符串里不要出现换行、制表符、emoji、表格符号「|」、反引号。
需要换行就用空格代替。违反这条会导致网关解析失败、评审白跑。
JSON 结构：
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
判据逐条判 PASS / FAIL / 未证实，一条不跳。默认结论 changes_requested。
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

# 输出纪律（很重要）
只允许输出一个 JSON 对象，不要 markdown 代码块，不要任何前后缀说明文字。
字符串里不要出现换行、制表符、emoji、表格符号「|」、反引号。
需要换行就用空格代替。违反这条会导致网关解析失败、评审白跑。
JSON 结构：
{"verdict":"approved|changes_requested",
 "rubric":[{"item":"判据原文","result":"PASS|FAIL|UNVERIFIED","evidence":"定位或空"}],
 "findings":[{"severity":"high|med|low","location":"章节或段落",
              "issue":"一句话","recommendation":"一句话"}],
 "questions_for_author":[]}

severity 口径：high = 事实错误 / 结论无依据 / 关键风险漏写；med = 论证不完整；low = 表述歧义。"""


# ---------------------------------------------------------------- 纯文本渲染

# 刻意不用 emoji 与表格：这两样是客户端 strict 解析下最容易炸的东西。
_MARK = {"PASS": "PASS", "FAIL": "FAIL", "UNVERIFIED": "UNVER", "": "-"}
_VERDICT_TXT = {"approved": "APPROVED", "changes_requested": "CHANGES_REQUESTED"}


def _clean(s: Any, limit: int = 400) -> str:
    """压成一行、去掉控制字符与分隔符，避免破坏外层文本结构。"""
    if s is None:
        return ""
    t = str(s)
    t = t.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    t = t.replace("|", "/").replace("`", "'")
    t = re.sub(r"[\x00-\x1f\x7f]", "", t)
    t = re.sub(r"\s{2,}", " ", t).strip()
    return t[:limit]


def render_plain(v: dict, rnd: int) -> str:
    verdict = _VERDICT_TXT.get(str(v.get("verdict", "")).lower(), "CHANGES_REQUESTED")
    findings = v.get("findings") or []
    if not isinstance(findings, list):
        findings = []

    def sev(f):
        return str(f.get("severity", "")).lower() if isinstance(f, dict) else ""

    high = [f for f in findings if sev(f) == "high"]
    med = [f for f in findings if sev(f) == "med"]
    low = [f for f in findings if sev(f) not in ("high", "med")]

    out = [
        f"VERDICT: {verdict}",
        f"ROUND: {rnd}/{MAX_ROUND}",
        f"COUNTS: high={len(high)} med={len(med)} low={len(low)}",
        "-" * 40,
    ]

    rub = v.get("rubric") or []
    if isinstance(rub, list) and rub:
        bad = [r for r in rub if isinstance(r, dict)
               and str(r.get("result", "")).upper() in ("FAIL", "UNVERIFIED")]
        out.append(f"RUBRIC: {len(rub) - len(bad)}/{len(rub)} passed")
        for r in rub:
            if not isinstance(r, dict):
                continue
            mark = _MARK.get(str(r.get("result", "")).upper(), "-")
            line = f"  [{mark}] {_clean(r.get('item'), 240)}"
            if r.get("evidence"):
                line += f"  <- {_clean(r.get('evidence'), 160)}"
            out.append(line)
        out.append("-" * 40)

    for label, group in (("HIGH", high), ("MED", med), ("LOW", low)):
        if not group:
            continue
        out.append(f"{label}:")
        for i, f in enumerate(group, 1):
            if not isinstance(f, dict):
                continue
            loc = _clean(f.get("location"), 120) or "未定位"
            out.append(f"  {i}. [{loc}] {_clean(f.get('issue'), 500)}")
            if f.get("recommendation"):
                out.append(f"     FIX: {_clean(f.get('recommendation'), 500)}")
        out.append("-" * 40)

    q = v.get("questions_for_author") or []
    if isinstance(q, list) and q:
        out.append("QUESTIONS_FOR_AUTHOR:")
        for x in q:
            out.append(f"  - {_clean(x, 400)}")
        out.append("-" * 40)

    if high and rnd > MAX_ROUND:
        out.append("ACTION: STOP")
        out.append(f"已到 {MAX_ROUND} 轮上限，禁止再调 review。把上面的 "
                   f"QUESTIONS_FOR_AUTHOR 原样交给人裁决，不要自行拍板。")
    elif high:
        out.append("ACTION: REVISE")
        out.append(f"逐条改掉上面 {len(high)} 条 HIGH，改完带同一个 session_id "
                   f"再调一次 review。只交付修改后的内容，不要解释、不要辩护。")
        out.append("代码类任务改完可先调 collect_evidence 让机器过一遍闸门。")
    else:
        out.append("ACTION: SHIP")
        out.append("可以交付。交人前最后确认一件事：证据里 SKIP 的项是真的不适用，"
                   "还是采集器没跑起来。")

    return "\n".join(out)


def clip(text: str, limit: int = MAX_TEXT_BYTES) -> str:
    """超体积就砍尾部并明说，绝不静默截断——被砍掉的是低优先级证据。"""
    b = text.encode("utf-8")
    if len(b) <= limit:
        return text
    head = b[: int(limit * 0.8)].decode("utf-8", errors="ignore")
    return (head + f"\n\n...[输出过长，已省略尾部约 {len(b) - limit} 字节。"
                   f"需要看全量请调 get_findings 或调小 collect_evidence 的 profiles]")

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
LOCK = threading.RLock()          # RLock：工具内部会二次加锁
POOL = ThreadPoolExecutor(max_workers=2)

# 进程内序列号：每次 tools/call 自增，用于把服务端日志和客户端报错对上号
_SEQ = 0


def get_session(sid: str | None) -> Session:
    sid = sid or "default"
    with LOCK:
        return SESS.setdefault(sid, Session(sid))


def next_seq() -> int:
    global _SEQ
    with LOCK:
        _SEQ += 1
        return _SEQ


# ---------------------------------------------------------------- 混元调用

class HYError(RuntimeError):
    pass


def call_hunyuan(system: str, user: str, timeout: int = REVIEW_TIMEOUT) -> str:
    """并发有限且主子账号共享，退避重试不是可选项。"""
    if hy is None:
        raise HYError("HUNYUAN_API_KEY 未配置。在同级 .env 写一行 HUNYUAN_API_KEY=你的key")
    last = None
    for i in range(HY_RETRY):
        try:
            r = hy.chat.completions.create(
                model=HY_MODEL, temperature=0.2, timeout=timeout,
                max_tokens=HY_MAX_TOKENS,
                messages=[{"role": "system", "content": system},
                          {"role": "user", "content": user}],
            )
            msg = r.choices[0].message
            # 部分 OpenAI 兼容网关把 content 放进 reasoning_content / delta，
            # 这里做一次兜底取值，避免拿到 None 就当失败重试。
            content = getattr(msg, "content", None)
            if not content:
                content = getattr(msg, "reasoning_content", None) or ""
                if not content and isinstance(msg, dict):
                    content = msg.get("content") or msg.get("reasoning_content") or ""
            return (content or "").strip()
        except Exception as e:                                # noqa: BLE001
            last = e
            if i == HY_RETRY - 1:
                break
            time.sleep(min(2 ** i * 1.5, 20))
    raise HYError(f"混元调用失败（重试 {HY_RETRY} 次）：{type(last).__name__}: {last}")


# 控制字符清洗：模型偶尔会在字符串里吐真实换行/制表符，那会让 JSON 非法。
_CTRL = {0x08: "\\b", 0x09: "\\t", 0x0A: "\\n", 0x0B: "\\u000b",
         0x0C: "\\f", 0x0D: "\\r"}


def _sanitize_for_json(txt: str) -> str:
    out = []
    for ch in txt:
        o = ord(ch)
        if o in _CTRL:
            out.append(_CTRL[o])
        elif o < 0x20 or o == 0x7F:
            out.append(f"\\u{o:04x}")
        else:
            out.append(ch)
    return "".join(out)


def _balance_scan(txt: str) -> list[str]:
    """返回所有括号平衡、且能 parse 成 dict 的候选片段（长的优先）。"""
    cands = []
    for a in (m.start() for m in re.finditer(r"\{", txt)):
        d = 0
        for i, c in enumerate(txt[a:], a):
            if c == "{":
                d += 1
            elif c == "}":
                d -= 1
                if d == 0:
                    try:
                        obj = json.loads(txt[a:i + 1])
                    except json.JSONDecodeError:
                        break
                    if isinstance(obj, dict):
                        cands.append(txt[a:i + 1])
                    break
    cands.sort(key=len, reverse=True)
    return cands


def extract_json(txt: str) -> dict:
    """
    三级兜底，尽量不让一次评审因为格式问题白跑：
      1. 原文直接扫平衡括号
      2. 剥离 ```json 围栏后再扫
      3. 清洗控制字符后对最长候选做一次容错 parse
    仍失败才抛错——此时把原文前 400 字回给调用方，便于定位。
    """
    if not txt:
        raise HYError("模型返回空内容（可能触发内容过滤或 max_tokens 不足）")

    for candidate in (txt,
                      re.sub(r"^\s*```[a-zA-Z0-9_-]*\s*|\s*```\s*$", "", txt,
                             flags=re.S),
                      _sanitize_for_json(txt)):
        for frag in _balance_scan(candidate):
            if "verdict" in frag:
                return json.loads(frag)

    # 最后一把：取最长候选，容错 loads（容忍尾逗号、单引号）
    longest = max(_balance_scan(_sanitize_for_json(txt)) or [""], key=len)
    if longest:
        try:
            return json.loads(longest)
        except json.JSONDecodeError:
            pass
    raise HYError("评审输出里找不到合法 JSON（需含 verdict 字段）。"
                  f"模型原文前 400 字：{_clean(txt, 400)}")

# ---------------------------------------------------------------- 响应封装

def tool_result(text: str, is_error: bool = False,
                structured: dict | None = None) -> dict:
    """
    MCP tools/call 的 result。

    PLAIN_TEXT=True（默认）时只回 content + isError —— 这两个字段是所有版本
    客户端都会认的最小集合，豆包报 invalid raw MCP result 的根因就是
    带了 structuredContent 却没有配套 outputSchema，客户端无法校验。
    需要结构化字段的客户端（Claude Code / Codex 等）设 MCP_PLAIN_TEXT=0 打开。
    """
    body = clip(text)
    d: dict[str, Any] = {
        "content": [{"type": "text", "text": body}],
        "isError": is_error,
    }
    if not PLAIN_TEXT and structured is not None:
        safe = {}
        for k, v in structured.items():
            if isinstance(v, (str, int, float, bool, type(None))):
                safe[k] = v
            elif isinstance(v, (list, dict)):
                safe[k] = json.loads(json.dumps(v, ensure_ascii=False,
                                                default=str))
            else:
                safe[k] = str(v)
        d["structuredContent"] = safe
    return d


# 发出去之前自己 parse 一次：炸在服务端日志里，别炸在客户端界面上
def _selfcheck(seq: int, name: str, res: dict) -> None:
    try:
        raw = json.dumps(res, ensure_ascii=False)
        raw.encode("utf-8")
    except Exception as e:                                   # noqa: BLE001
        print(f"[seq {seq}] SERIALIZE-FAIL {name}: {type(e).__name__}: {e}",
              flush=True)
        return
    print(f"[seq {seq}] {name} ok bytes={len(raw.encode('utf-8'))} "
          f"isError={res.get('isError')} "
          f"preview={_clean(res['content'][0]['text'], 90)}", flush=True)


# ---------------------------------------------------------------- 工具实现

def load_checks(kind: str) -> str:
    try:
        txt = open(CHECKS_PATH, encoding="utf-8").read()
    except OSError as e:
        return ("（判据文件读不到：%s；用通用口径：事实可溯源、逻辑无跳跃、"
                "边界无遗漏、断言有证据）" % e)
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
        return tool_result(f"项目路径不存在：{root}", is_error=True)

    res = evidence.collect(root=root, base=base, profiles=profiles,
                           run_tests=run_tests, do_tidy=do_tidy)
    block = evidence.to_review_block(res)

    sid = args.get("session_id")
    if sid:
        with LOCK:
            get_session(sid).last_evidence = block

    gates = res.get("gates") or {}
    head = (
        f"证据采集完成，用时 {res.get('elapsed')}s。\n"
        f"硬性闸门：{'未过 -> ' + ', '.join(res.get('hard_fail') or []) if res.get('hard_fail') else '全部通过'}\n"
        f"改动文件 {len(res.get('changed') or [])} 个。\n"
        f"闸门明细：{', '.join(f'{k}={v}' for k, v in list(gates.items())[:12])}\n\n"
        f"接下来必须调用 review 工具，把待审内容通过 content 传入；"
        f"证据已按 session 缓存，review 会自动带上，不用你复述。\n\n"
    )
    return tool_result(head + block, structured={
        "project_root": res.get("project", {}).get("root", root),
        "gates": gates,
        "hard_fail": res.get("hard_fail") or [],
        "changed_count": len(res.get("changed") or []),
        "elapsed": res.get("elapsed")})


def tool_review(args: dict) -> dict:
    content = (args.get("content") or "").strip()
    if not content:
        return tool_result("content 为空，没有可审的内容。", is_error=True)

    kind = args.get("kind")
    if kind not in ("code", "doc"):
        # 缺省推断：带了 project_root 或已经缓存过证据才走 C++ 判据，
        # 否则一律按文档审——避免文档任务被 C++ 条目误伤。
        s0 = get_session(args.get("session_id"))
        cached = bool(s0.last_evidence)
        kind = "code" if (args.get("project_root") or cached) else "doc"

    s = get_session(args.get("session_id"))
    with LOCK:
        if s.round >= MAX_ROUND:
            return tool_result(
                f"会话 {s.sid} 已用满 {MAX_ROUND} 轮，网关拒绝开启第 {s.round + 1} 轮。\n"
                f"这是硬规则：继续审只是在加噪声和烧钱。\n"
                f"请调 get_findings 取出最后一轮结论，把里面的 QUESTIONS_FOR_AUTHOR "
                f"连同你的判断一起交给人裁决。\n"
                f"确实要重开一轮，先调 reset_session。", is_error=True)
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
    user = clip("\n\n".join(parts), limit=int(MAX_TEXT_BYTES * 1.5))

    t0 = time.time()
    try:
        raw = call_hunyuan(sys_p, user)
        v = extract_json(raw)
    except HYError as e:
        with LOCK:
            s.round -= 1              # 失败不算一轮，留额度给重试
        print(f"[review] FAIL session={s.sid} round={rnd} "
              f"{round(time.time() - t0, 1)}s {e}", flush=True)
        return tool_result(
            f"评审没跑成：{e}\n\n"
            f"**不要自行判定通过**，也不要把这次失败当成评审意见。"
            f"轮次未消耗，修好后可带同一 session_id 原样重试。", is_error=True)

    with LOCK:
        s.hist.append(json.dumps(v, ensure_ascii=False))

    findings = v.get("findings")
    if not isinstance(findings, list):
        findings = [findings] if isinstance(findings, dict) else []
    high_n = sum(1 for f in findings
                 if str(f.get("severity", "")).lower() == "high")
    print(f"[review] OK session={s.sid} round={rnd} "
          f"{round(time.time() - t0, 1)}s verdict={v.get('verdict')} high={high_n}",
          flush=True)

    return tool_result(render_plain(v, rnd), structured={
        "verdict": str(v.get("verdict", "changes_requested")),
        "round": rnd,
        "high": high_n,
        "exhausted": rnd >= MAX_ROUND,
        "questions_for_author": v.get("questions_for_author") or []})


def tool_get_findings(args: dict) -> dict:
    s = get_session(args.get("session_id"))
    with LOCK:
        h = list(s.hist)
        cached = bool(s.last_evidence)
    if not h:
        return tool_result(
            f"会话 {s.sid} 还没有评审记录（当前轮次 {s.round}，"
            f"证据缓存 {'有' if cached else '无'}）。\n"
            f"代码类任务先调 collect_evidence 再调 review；"
            f"文档类任务直接调 review 并传 kind=doc。\n"
            f"如果刚才那次 review 报了错，那次没有落库，属于正常现象。")
    parts = [f"会话 {s.sid} 共 {len(h)} 轮评审："]
    for i, x in enumerate(h, 1):
        parts.append(f"[第 {i} 轮]\n{x}")
    if cached:
        parts.append("[提示] 本会话还有一份未消费的证据缓存，"
                     "下一次 review 会把它带上。")
    return tool_result("\n\n".join(parts),
                       structured={"session_id": s.sid, "rounds": len(h)})


def tool_reset(args: dict) -> dict:
    sid = args.get("session_id") or "default"
    with LOCK:
        existed = SESS.pop(sid, None) is not None
    return tool_result(
        f"会话 {sid} 已{'清空' if existed else '本来就不存在'}，轮次归零。"
        f"下一轮 review 会从第 1 轮重新计数。")


def tool_whoami(_: dict) -> dict:
    ready = False
    detail = "HUNYUAN_API_KEY 未配置"
    if hy is not None:
        t0 = time.time()
        try:
            call_hunyuan("回复两个字：就绪", "ping", timeout=60)
            ready = True
            detail = f"模型连通正常（{round(time.time() - t0, 1)}s）"
        except HYError as e:
            detail = f"模型连通失败：{e}"
    checks_ok = os.path.isfile(CHECKS_PATH)
    lines = [
        f"评审员：混元 {HY_MODEL}",
        f"端点：{HY_BASE_URL}",
        f"连通性：{detail}",
        f"模式：{'纯文本返回（structuredContent 关闭）' if PLAIN_TEXT else '结构化返回开启'}",
        f"项目根：{PROJECT_ROOT}",
        f"基准分支：{GIT_BASE}",
        f"轮次上限：{MAX_ROUND}",
        f"判据文件：{CHECKS_PATH}{'（正常）' if checks_ok else '（缺失，将用通用口径）'}",
        f"采集器：cmake / clang-tidy / clang-format / ctest / ASan+UBSan / TSan / nm",
        f"在册会话：{len(SESS)} 个",
        f"证据采集只读，不会改你的文件，也不会 commit。",
    ]
    return tool_result("\n".join(lines), structured={
        "ready": ready, "model": HY_MODEL, "base_url": HY_BASE_URL,
        "plain_text": PLAIN_TEXT, "project_root": PROJECT_ROOT,
        "git_base": GIT_BASE, "max_round": MAX_ROUND,
        "checks_loaded": checks_ok, "sessions": len(SESS)})

# ---------------------------------------------------------------- MCP 协议

TOOLS: list[dict] = [
    {"name": "collect_evidence",
     "description":
         "【代码类任务，审前必调】跑机器闸门并采集证据：CMake 配置、编译（-Werror）、"
         "clang-tidy 增量、clang-format、ctest、ASan+UBSan、TSan、ABI 符号清单，"
         "头文件改动自动展开到引用它的翻译单元。采集器只读，不改文件不 commit。"
         "结果按 session 缓存，随后的 review 会自动带上。",
     "inputSchema": {"type": "object", "properties": {
         "project_root": {"type": "string",
                          "description": "仓库根目录绝对路径，缺省用服务端 PROJECT_ROOT"},
         "base": {"type": "string", "description": "增量 diff 的参照分支，缺省 main"},
         "profiles": {"type": "array", "items": {"type": "string"},
                      "description": "编译闸门组合，可多选：plain / asan / tsan。缺省 [\"plain\"]"},
         "run_tests": {"type": "boolean", "description": "是否跑测试，缺省 true"},
         "tidy": {"type": "boolean", "description": "是否跑 clang-tidy，缺省 true"},
         "session_id": {"type": "string",
                        "description": "同一任务全程传同一个值，用于缓存证据与只审增量"}},
         "additionalProperties": False}},
    {"name": "review",
     "description":
         "把产出交混元评审，返回逐条判据结果 + 分级问题 + 改法（纯文本）。"
         "有 HIGH 级问题就必须改完再调一次（带同一 session_id，本轮只审增量）。"
         f"超过 {MAX_ROUND} 轮网关会直接拒绝，把策略问题交给人。",
     "inputSchema": {"type": "object", "properties": {
         "content": {"type": "string", "description": "待审的完整产出（代码 diff 或文档正文）"},
         "kind": {"type": "string", "enum": ["code", "doc"],
                  "description": "评审口径：code 走 C++ 判据，doc 走文档判据。缺省自动推断"},
         "session_id": {"type": "string",
                        "description": "同一任务多次评审传同一个值，用于只审增量与轮次计数"},
         "project_root": {"type": "string", "description": "代码类任务填仓库根"}},
         "required": ["content"], "additionalProperties": False}},
    {"name": "get_findings",
     "description": "读本会话全部轮次的评审原始 JSON，用于复盘或交给用户看。",
     "inputSchema": {"type": "object", "properties": {
         "session_id": {"type": "string"}},
         "additionalProperties": False}},
    {"name": "reset_session",
     "description": "清空会话的轮次与历史，从头开始一轮新的评审。",
     "inputSchema": {"type": "object", "properties": {
         "session_id": {"type": "string"}}, "additionalProperties": False}},
    {"name": "whoami",
     "description": "查评审员身份：模型、端点、连通性、项目根、轮次上限、判据文件状态。先用它自检环境。",
     "inputSchema": {"type": "object", "properties": {},
                     "additionalProperties": False}},
]

# 网关层提示：支持该字段的客户端（Claude Code / Codex / Cline / Cursor 等）会据此
# 要求模型必须调用；不认的客户端直接忽略，靠 INSTRUCTIONS 里的约定兜底。
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

INSTRUCTIONS = """这是混元驱动的只读评审员。工作铁律：

1. 产出任何实质性内容后，代码类必须先调 collect_evidence 再调 review，
   拿到 VERDICT: APPROVED 才算完成。不许自己给自己盖章。
2. 代码类任务一律先 collect_evidence：没有机器证据，评审员只能猜，
   「编译通过」「测试全绿」这类断言不许自己写。
3. review 返回 ACTION: REVISE 时，逐条改掉 HIGH，带同一个 session_id
   再调 review。只审增量，不要重做整篇。
4. 最多 2 轮，第 3 轮网关会直接拒绝。此时把 QUESTIONS_FOR_AUTHOR
   原样交给人，不要自行拍板。
5. 评审意见里的 MED/LOW 若你判断只是风格偏好，可以保留自己的写法，
   但要在交付说明里写明「评审提出 X，我保留，理由是 Y」。
6. 工具返回 isError 为 true 时，那是工具出错了，不是评审结论。
   绝不许把「工具调用失败」当成「评审通过」来处理。
"""

app = FastAPI(title="hunyuan-review-mcp", version="1.1")


def _ok(i: Any, result: Any) -> JSONResponse:
    return JSONResponse({"jsonrpc": "2.0", "id": i, "result": result})


def _err(i: Any, code: int, msg: str, data: Any = None) -> JSONResponse:
    body: dict[str, Any] = {"code": code, "message": _clean(msg, 300)}
    if data is not None:
        body["data"] = data
    return JSONResponse({"jsonrpc": "2.0", "id": i, "error": body})


@app.get("/health")
async def health():
    return {"ok": True, "model": HY_MODEL, "base_url": HY_BASE_URL,
            "key_configured": bool(HY_KEY), "plain_text": PLAIN_TEXT,
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
    body = await request.json()
    r = await dispatch(body)
    return r if isinstance(r, JSONResponse) else JSONResponse({"ok": True})


@app.post("/mcp")
async def mcp(request: Request):
    try:
        body = await request.json()
    except Exception as e:                                   # noqa: BLE001
        return _err(None, -32700, f"请求体不是合法 JSON：{e}")
    if isinstance(body, list):
        # JSON-RPC 批量请求：逐个派发，聚合返回
        return JSONResponse([(await dispatch(b)).body for b in body
                             if isinstance(b, dict)])
    return await dispatch(body)


async def dispatch(body: dict) -> JSONResponse:
    if not isinstance(body, dict):
        return _err(None, -32600, "请求必须是 JSON 对象")

    m = body.get("method")
    i = body.get("id")
    seq = next_seq()
    t0 = time.time()

    if m == "initialize":
        # 版本协商：客户端报什么就尽量回什么，实在不认识才回我们最高的。
        # 原先硬编码 2025-03-26，遇到只认 2024-11-05 的客户端会直接握手失败。
        want = str(body.get("protocolVersion") or "")
        picked = want if want in SUPPORTED_PROTOCOL else SUPPORTED_PROTOCOL[0]
        print(f"[seq {seq}] initialize client={_clean(want, 40)} -> {picked}",
              flush=True)
        return _ok(i, {
            "protocolVersion": picked,
            "capabilities": {"tools": {"listChanged": False},
                             "resources": {"subscribe": False, "listChanged": False}},
            "serverInfo": {"name": "hunyuan-reviewer", "version": "1.1"},
            "instructions": INSTRUCTIONS,
        })

    if m in ("notifications/initialized", "notifications/cancelled"):
        print(f"[seq {seq}] {m}", flush=True)
        return JSONResponse({}, status_code=202)

    if m == "ping":
        return _ok(i, {})

    if m == "tools/list":
        tools = TOOLS
        # 老协议（2024-11-05）不认 additionalProperties 之外的扩展字段，
        # 但 requiredTools 是我们自己加的提示，留着对老客户端无害，
        # 真出问题时可以用 MCP_STRICT=1 关掉。
        if os.getenv("MCP_STRICT") == "1":
            tools = [{k: v for k, v in t.items()
                      if k in ("name", "description", "inputSchema")}
                     for t in TOOLS]
        return _ok(i, {"tools": tools, "requiredTools": REQUIRED_TOOLS})

    if m == "tools/call":
        p = body.get("params") or {}
        name = p.get("name")
        fn = HANDLERS.get(name)
        if fn is None:
            print(f"[seq {seq}] tools/call UNKNOWN name={_clean(name, 60)}",
                  flush=True)
            return _err(i, -32602,
                        f"未知工具：{name}。可用：{', '.join(HANDLERS)}")
        args = p.get("arguments") or {}
        if not isinstance(args, dict):
            args = {}
        print(f"[seq {seq}] tools/call {name} "
              f"args_keys={sorted(args.keys())} "
              f"content_bytes={len(str(args.get('content','')).encode('utf-8'))} "
              f"session={_clean(args.get('session_id'), 40)}", flush=True)

        try:
            if name == "collect_evidence":
                res = await asyncio.wait_for(
                    asyncio.get_running_loop().run_in_executor(POOL, fn, args),
                    timeout=3600)
            else:
                res = await asyncio.get_running_loop().run_in_executor(POOL, fn, args)
        except asyncio.TimeoutError:
            return _err(i, -32000,
                        "证据采集超过 3600s 已终止。先只跑 profiles=[\"plain\"]，"
                        "或把 run_tests 设 false 缩小范围。")
        except Exception as e:                               # noqa: BLE001
            print(f"[seq {seq}] tools/call CRASH {name}: "
                  f"{type(e).__name__}: {e}", flush=True)
            # 工具内部异常也走正常 result + isError，不让整个 JSON-RPC 层崩掉
            res = tool_result(
                f"工具 {name} 内部异常：{type(e).__name__}: {e}\n"
                f"请把这条报错连同 seq {seq} 一起反馈。评审结论未产生，"
                f"不要自行判定通过。", is_error=True)

        _selfcheck(seq, name, res)
        print(f"[seq {seq}] done {round(time.time() - t0, 1)}s", flush=True)
        return _ok(i, res)

    if m == "resources/list":
        return _ok(i, {"resources": RESOURCES})

    if m == "resources/read":
        uri = (body.get("params") or {}).get("uri")
        kind = "code" if uri == "checks://code" else "doc"
        return _ok(i, {"contents": [{"uri": uri, "mimeType": "text/markdown",
                                     "text": load_checks(kind)}]})

    if i is None:
        print(f"[seq {seq}] notify ignored {_clean(m, 60)}", flush=True)
        return JSONResponse({}, status_code=202)

    print(f"[seq {seq}] unsupported method {_clean(m, 60)}", flush=True)
    return _err(i, -32601, f"不支持的方法：{m}")


if __name__ == "__main__":
    if not HY_KEY:
        print(f"⚠️  HUNYUAN_API_KEY 未设置，review 工具会直接报错，证据采集仍可用。")
        print(f"    在 {os.path.join(_HERE, '.env')} 里写一行：HUNYUAN_API_KEY=你的key")
    print(f"评审模型：{HY_MODEL}")
    print(f"端点：{HY_BASE_URL}")
    print(f"返回模式：{'纯文本（推荐，客户端兼容性最好）' if PLAIN_TEXT else '结构化'}")
    print(f"项目根：{PROJECT_ROOT}")
    print(f"基准分支：{GIT_BASE}    轮次上限：{MAX_ROUND}")
    print(f"判据文件：{'正常' if os.path.isfile(CHECKS_PATH) else '缺失 -> ' + CHECKS_PATH}")
    uvicorn.run(app, host=os.getenv("BIND", "127.0.0.1"),
                port=int(os.getenv("PORT", "8080")), log_level="info")
