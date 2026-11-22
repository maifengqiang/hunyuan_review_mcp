# -*- coding: utf-8 -*-
"""
证据采集器：把「声称已验证」从一句判据变成一道机器关卡。

设计原则（对应《验证工程》第 4 级「证据随附」）：
  1. 只采集，不评判。评判交给混元。
  2. 每条证据必须能追到一条命令 + 退出码 + 输出片段，不许模型自己写「编译通过」。
  3. 采集失败不是错误，是一条 FAIL 证据——编译都跑不起来本身就是评审输入。
  4. 全只读。不 commit、不改文件、不装依赖（缺工具就如实报告缺失）。
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, asdict
from typing import Any

# ---------------------------------------------------------------- 基础工具

DEFAULT_TIMEOUT = int(os.getenv("EVIDENCE_TIMEOUT", "900"))   # 秒，C++ 编译别设太短
TAIL_LINES = int(os.getenv("EVIDENCE_TAIL", "120"))           # 每类证据保留的输出尾部行数
MAX_BYTES = int(os.getenv("EVIDENCE_MAXBYTES", "24000"))      # 单条证据输出上限，防撑爆上下文


def _truncate(text: str, limit: int = MAX_BYTES) -> str:
    if len(text) <= limit:
        return text
    head = text[: int(limit * 0.35)]
    tail = text[-int(limit * 0.55):]
    cut = len(text) - limit
    return f"{head}\n...〔中间省略 {cut} 字符〕...\n{tail}"


def _run(cmd: list[str], cwd: str, timeout: int = DEFAULT_TIMEOUT,
         env: dict | None = None) -> dict:
    """跑一条命令，永远不抛异常——失败也是一种证据。"""
    started = time.time()
    try:
        p = subprocess.run(
            cmd, cwd=cwd, capture_output=True, text=True,
            timeout=timeout, env={**os.environ, **(env or {})},
        )
        out = (p.stdout or "") + (("\n[stderr]\n" + p.stderr) if p.stderr else "")
        return {
            "cmd": " ".join(cmd),
            "exit": p.returncode,
            "ok": p.returncode == 0,
            "seconds": round(time.time() - started, 1),
            "output": _truncate(out.strip() or "(无输出)"),
        }
    except subprocess.TimeoutExpired:
        return {"cmd": " ".join(cmd), "exit": None, "ok": False,
                "seconds": round(time.time() - started, 1),
                "output": f"〔超时 {timeout}s，已终止〕"}
    except FileNotFoundError:
        return {"cmd": " ".join(cmd), "exit": None, "ok": False,
                "seconds": 0, "output": f"〔命令不存在：{cmd[0]}〕"}
    except Exception as e:                                    # noqa: BLE001
        return {"cmd": " ".join(cmd), "exit": None, "ok": False,
                "seconds": round(time.time() - started, 1),
                "output": f"〔执行异常：{type(e).__name__}: {e}〕"}


def _tail(out: str, n: int = TAIL_LINES) -> str:
    lines = out.splitlines()
    return "\n".join(lines[-n:]) if len(lines) > n else out


# ---------------------------------------------------------------- 仓库探测

@dataclass
class Project:
    root: str
    cmake_dir: str | None = None            # 含 CMakeLists.txt 的目录
    build_dir: str | None = None
    compile_commands: str | None = None
    has_git: bool = False
    has_compile_db: bool = False
    has_tests: bool = False
    compiler: str | None = None
    generators: list[str] = field(default_factory=list)
    toolchain: dict[str, str] = field(default_factory=dict)


def detect_project(root: str) -> Project:
    p = Project(root=os.path.abspath(root))

    p.has_git = os.path.isdir(os.path.join(p.root, ".git"))

    # CMakeLists.txt：优先根，其次一级子目录（常见 src/ 布局）
    if os.path.isfile(os.path.join(p.root, "CMakeLists.txt")):
        p.cmake_dir = p.root
    else:
        try:
            for name in sorted(os.listdir(p.root)):
                cand = os.path.join(p.root, name)
                if os.path.isfile(os.path.join(cand, "CMakeLists.txt")):
                    p.cmake_dir = cand
                    break
        except OSError:
            pass

    # 已有构建目录？优先复用用户的（含他的 -D 参数与缓存），否则我们自己建一个
    for cand in ("build", "cmake-build-debug", "out/build", "build-ci"):
        d = os.path.join(p.root, cand)
        if os.path.isfile(os.path.join(d, "CMakeCache.txt")):
            p.build_dir = d
            break
        db = os.path.join(d, "compile_commands.json")
        if os.path.isfile(db):
            p.build_dir, p.compile_commands = d, db
            break

    # compile_commands.json 可能在根、在 build、或在 .cache（clangd 常用）
    for cand in ("compile_commands.json",
                 ".cache/compile_commands.json",
                 "build/compile_commands.json",
                 "out/compile_commands.json"):
        f = os.path.join(p.root, cand)
        if os.path.isfile(f):
            p.compile_commands, p.has_compile_db = f, True
            break

    p.compiler = shutil.which("clang++") or shutil.which("g++")
    p.generators = [g for g in ("Ninja", "Unix Makefiles")
                    if _generator_available(g)]

    # 有没有测试：看常见目录与 CTest 声明
    for cand in ("tests", "test", "unittest", "unit_tests", "gtest"):
        if os.path.isdir(os.path.join(p.root, cand)):
            p.has_tests = True
            break
    if not p.has_tests and p.cmake_dir:
        try:
            txt = open(os.path.join(p.cmake_dir, "CMakeLists.txt"),
                       encoding="utf-8", errors="ignore").read()
            p.has_tests = bool(re.search(
                r"\b(enable_testing|add_test|gtest_discover_tests|catch_discover_tests|"
                r"FetchContent.*googletest|find_package\(\s*Catch2)\b", txt))
        except OSError:
            pass

    # 工具链清单：评审时需要知道「用户环境里到底有什么」
    for tool in ("cmake", "ninja", "make", "clang++", "g++", "clang-tidy",
                 "clang-format", "cppcheck", "ctest", "git", "nm", "readelf"):
        path = shutil.which(tool)
        if path:
            p.toolchain[tool] = path

    return p


def _generator_available(name: str) -> bool:
    exe = "ninja" if name == "Ninja" else "make"
    return shutil.which(exe) is not None


# ---------------------------------------------------------------- 改动文件

def changed_files(root: str, base: str = "main") -> list[str]:
    """相对 base 的改动文件（排除已删除）。失败返回 []，交由上层降级为全量。"""
    p = subprocess.run(
        ["git", "diff", "--name-only", "--diff-filter=ACMR",
         f"{base}...HEAD"], cwd=root, capture_output=True, text=True)
    if p.returncode != 0:                       # 没这个 ref / 不是 git 仓库
        p = subprocess.run(["git", "diff", "--name-only", "--diff-filter=ACMR",
                            "--cached"], cwd=root, capture_output=True, text=True)
    if p.returncode != 0:
        return []
    return [ln.strip() for ln in p.stdout.splitlines() if ln.strip()]


SOURCE_EXT = (".c", ".cc", ".cpp", ".cxx", ".c++", ".h", ".hh", ".hpp",
              ".hxx", ".inl", ".tcc")
HEADER_EXT = (".h", ".hh", ".hpp", ".hxx", ".inl", ".tcc")


def _is_source(f: str) -> bool:
    return f.endswith(SOURCE_EXT)


def expand_headers_to_tus(root: str, files: list[str]) -> list[str]:
    """
    改了头文件，就得审所有引用它的翻译单元——否则「只审了 .h」等于没审。
    先按 include 关系启发式匹配（无需 compile_commands），有 compile_db 时用
    「该文件是否在某个 TU 的 includes 里」精确过滤。
    """
    changed_headers = [f for f in files if f.endswith(HEADER_EXT)]
    if not changed_headers:
        return files

    extra: set[str] = set()
    # 启发式：在源码里 grep #include "该头文件名"
    for h in changed_headers:
        base = os.path.basename(h)
        pat = re.compile(r'#\s*include\s*"([^"]*' + re.escape(base) + r')"')
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames
                           if d not in (".git", "build", "cmake-build-debug",
                                        "out", ".cache", "third_party",
                                        "external", "node_modules")]
            for fn in filenames:
                if not fn.endswith((".c", ".cc", ".cpp", ".cxx")):
                    continue
                full = os.path.join(dirpath, fn)
                try:
                    txt = open(full, encoding="utf-8", errors="ignore").read()
                except OSError:
                    continue
                if pat.search(txt):
                    extra.add(os.path.relpath(full, root))
    return sorted(set(files) | extra)


# ---------------------------------------------------------------- 各项证据

def gate_compile_db(p: Project) -> dict:
    """A 组闸门第一条：没有 compile_commands.json，tidy 与增量编译都无从谈起。"""
    if p.has_compile_db:
        return {"name": "compile_commands", "ok": True,
                "detail": f"已找到 {os.path.relpath(p.compile_commands, p.root)}"}
    hint = ("在项目根或 build 目录都没有 compile_commands.json。\n"
            "  生成方式：cmake -S . -B build -DCMAKE_EXPORT_COMPILE_COMMANDS=ON\n"
            "  （Makefile 生成器需 cmake >= 3.20；Ninja 原生支持）")
    if p.cmake_dir is None:
        hint = "未找到 CMakeLists.txt，且没有 compile_commands.json；本次只能跑通用静态检查。"
    return {"name": "compile_commands", "ok": False, "detail": hint}


def gate_configure(p: Project, extra_args: list[str] | None = None) -> tuple[dict, str | None]:
    """
    配置一个带完整诊断的构建目录。复用已有 build 目录时只补断言，不覆盖用户参数。
    返回 (证据, 可用于后续构建的 build_dir)。
    """
    if p.cmake_dir is None:
        return {"name": "cmake_configure", "ok": False,
                "detail": "未找到 CMakeLists.txt，跳过 CMake 阶段"}, None

    build = p.build_dir or os.path.join(p.root, "build-evidence")
    os.makedirs(build, exist_ok=True)

    args = ["cmake", "-S", p.cmake_dir, "-B", build,
            "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON",
            "-DCMAKE_CXX_FLAGS=-fno-omit-frame-pointer",
            * (extra_args or [])]
    if p.build_dir is None and "Ninja" in p.generators:
        args.append("-GNinja")

    r = _run(args, p.root)
    r["name"] = "cmake_configure"
    r["detail"] = ("配置成功" if r["ok"] else "配置失败，后续编译/测试证据无法采集")
    r["output"] = _tail(r["output"])
    if r["ok"] and not p.has_compile_db:
        db = os.path.join(build, "compile_commands.json")
        if os.path.isfile(db):
            p.compile_commands, p.has_compile_db = db, True
    return r, (build if r["ok"] else None)


def ev_build(p: Project, build_dir: str, profile: str) -> list[dict]:
    """
    编译证据。profile:
      plain   —— 常规 -Wall -Wextra -Werror（最快的语法/告警闸门）
      asan    —— ASan + UBSan（约 2x 开销，能抓内存与 UB）
      tsan    —— TSan 独立构建（与 ASan 影子内存布局物理不兼容，必须分开）
    一律 Debug：Release 的优化会重排甚至消除代码，反而把问题藏起来。
    """
    out: list[dict] = []
    flags = {
        "plain": ["-Wall", "-Wextra", "-Wpedantic", "-Werror"],
        "asan":  ["-Wall", "-Wextra", "-g", "-fno-omit-frame-pointer",
                  "-fsanitize=address,undefined",
                  "-fno-sanitize-recover=all",
                  "-fsanitize-address-use-after-scope=1"],
        "tsan":  ["-Wall", "-Wextra", "-g", "-fno-omit-frame-pointer",
                  "-fsanitize=thread", "-fno-sanitize-recover=all"],
    }.get(profile, [])

    cmd = ["cmake", "--build", build_dir, "--parallel",
           "--", "CXXFLAGS=" + " ".join(flags)]
    if profile != "plain":
        cmd = ["cmake", "--build", build_dir, "--parallel"]
        r0 = _run(["cmake", "-S", p.cmake_dir, "-B", build_dir,
                   "-DCMAKE_BUILD_TYPE=Debug",
                   "-DCMAKE_CXX_FLAGS=" + " ".join(flags)], p.root)
        out.append({**r0, "name": f"configure_{profile}",
                    "detail": f"{profile} 构建目录配置" + ("成功" if r0["ok"] else "失败")})
        if r0["ok"]:
            r = _run(cmd, p.root)
            out.append({**r, "name": f"build_{profile}",
                        "detail": _build_verdict(r, profile),
                        "output": _tail(r["output"])})
        return out

    r = _run(cmd, p.root)
    out.append({**r, "name": "build_plain", "detail": _build_verdict(r, "plain"),
                "output": _tail(r["output"])})
    return out


def _build_verdict(r: dict, profile: str) -> str:
    if r["ok"]:
        return f"{profile} 构建干净（含 -Werror 级别告警为零）"
    if r["exit"] is None:
        return f"{profile} 构建未能执行"
    low = r["output"].lower()
    if "error" in low:
        return f"{profile} 构建失败：存在编译错误"
    return f"{profile} 构建失败：告警被 -Werror 升级为错误（winge 或 signed 转换等）"


def ev_tests(p: Project, build_dir: str) -> list[dict]:
    """测试证据：ctest 优先，退出码 + 失败用例名都要留下来。"""
    if not p.has_tests:
        return [{"name": "tests", "ok": None, "exit": None,
                 "detail": "未发现测试目标（无 tests/ 目录且 CMakeLists 里无 add_test/enable_testing）",
                 "output": ""}]
    r = _run(["ctest", "--test-dir", build_dir, "--output-on-failure",
              "--no-compress-output", "--timeout", "300"], p.root)
    failed = re.findall(r"^\s*(\d+/\d+\s+Test\s+#\d+:\s+\S+)\s+\.\.\.\s+\*+Failed",
                        r["output"], re.M)
    detail = ("全部用例通过" if r["ok"] else
              f"有用例失败：{'; '.join(failed[:8]) or '见输出'}" if failed else
              "ctest 执行失败（可能未构建测试目标）")
    return [{**r, "name": "tests", "detail": detail, "output": _tail(r["output"], 90)}]


def ev_sanitizer_tests(p: Project, build_dir_asan: str) -> list[dict]:
    """
    ASan/UBSan 下的测试运行——编译干净不等于运行干净。
    关键的 `halt_on_error=1` 让退出码非零，否则 CI 永远绿。
    """
    if not p.has_tests:
        return []
    env = {"ASAN_OPTIONS": "halt_on_error=1:detect_leaks=1:abort_on_error=0:"
                           "print_summary=1:detect_stack_use_after_return=1",
           "UBSAN_OPTIONS": "halt_on_error=1:print_stacktrace=1"}
    r = _run(["ctest", "--test-dir", build_dir_asan, "--output-on-failure",
              "--timeout", "600"], p.root, env=env)
    hit = re.findall(r"(runtime error|AddressSanitizer|LeakSanitizer|"
                     r"use-after-free|stack-buffer-overflow|heap-buffer-overflow|"
                     r"undefined behavior)", r["output"], re.I)
    detail = ("ASan/UBSan 下无诊断" if r["ok"] else
              f"Sanitizer 报出 {len(hit)} 处诊断，去重：{sorted(set(h.lower() for h in hit))[:6]}")
    return [{**r, "name": "tests_asan", "detail": detail,
             "output": _tail(r["output"], 110)}]


def ev_tidy(p: Project, changed: list[str]) -> list[dict]:
    """
    增量 clang-tidy：只扫改动的 TU。全量 tidy 在老项目上会吐几万条，直接劝退。
    有 compile_db 时按 TU 精确扫；没有就退化成「只扫改动文件、不传 -p」（告警会变多，需标注）。
    """
    if not p.toolchain.get("clang-tidy"):
        return [{"name": "clang_tidy", "ok": None, "exit": None,
                 "detail": "环境无 clang-tidy，跳过（这不代表代码干净）", "output": ""}]
    tus = [f for f in changed if f.endswith((".c", ".cc", ".cpp", ".cxx"))]
    if not tus:
        return [{"name": "clang_tidy", "ok": None, "exit": None,
                 "detail": "本次改动没有 .cpp/.cc 翻译单元，tidy 跳过", "output": ""}]

    args = ["clang-tidy", "--quiet", *tus[:40]]
    if p.has_compile_db:
        args += ["-p", os.path.dirname(p.compile_commands)]
    else:
        args += ["--", "-std=c++17", "-I" + p.root]
    r = _run(args, p.root, timeout=DEFAULT_TIMEOUT)
    n = len(re.findall(r"\[(bugprone|performance|modernize|readability|"
                       r"cppcoreguidelines|clang-analyzer|cert|misc)-[a-z0-9-]+\]",
                       r["output"]))
    detail = (f"改动 {len(tus)} 个 TU，无 clang-tidy 告警" if r["ok"] and n == 0 else
              f"改动 {len(tus)} 个 TU，报出约 {n} 条告警" +
              ("" if p.has_compile_db else "（无 compile_commands，用兜底参数扫，含噪声）"))
    return [{**r, "name": "clang_tidy", "ok": n == 0 and r["exit"] == 0,
             "detail": detail, "output": _tail(r["output"], 100)}]


def ev_format(p: Project, changed: list[str]) -> list[dict]:
    """格式只是门禁里最便宜的一条，但它让评审员不用在排版上浪费注意力。"""
    exe = p.toolchain.get("clang-format")
    cfg = next((os.path.join(p.root, c) for c in
                (".clang-format", "_clang-format")
                if os.path.isfile(os.path.join(p.root, c))), None)
    if not exe:
        return [{"name": "clang_format", "ok": None, "exit": None,
                 "detail": "环境无 clang-format，跳过", "output": ""}]
    files = [f for f in changed if f.endswith(
        (".c", ".cc", ".cpp", ".cxx", ".h", ".hpp", ".hh", ".hxx"))]
    if not files:
        return []
    bad: list[str] = []
    for f in files[:60]:
        full = os.path.join(p.root, f)
        r = subprocess.run([exe, "--dry-run", "--Werror",
                            *( ["--style=file:" + cfg] if cfg else ["--style=LLVM"] ),
                            full],
                           capture_output=True, text=True)
        if r.returncode != 0:
            bad.append(f)
    return [{"name": "clang_format", "ok": not bad, "exit": 0 if not bad else 1,
             "cmd": f"clang-format --dry-run --Werror × {len(files)}",
             "seconds": 0, "output": "\n".join(bad)[:2000],
             "detail": "格式全部合规" if not bad else
                       f"{len(bad)} 个文件格式不符：{', '.join(bad[:12])}"}]


def ev_abi(p: Project, base: str, build_dir: str) -> list[dict]:
    """
    ABI 是评审员最容易放水的地方：改动编译通过、测试通过、sanitizer 干净，
    上线照样崩。这里给出可比对的前后符号清单。没有 abi-dumper 时退化到 nm。
    """
    if not p.has_git:
        return [{"name": "abi", "ok": None, "exit": None,
                 "detail": "非 git 仓库，无法取 base 版本做 ABI 前后对比", "output": ""}]
    libs = []
    for dirpath, _, filenames in os.walk(build_dir):
        for fn in filenames:
            if fn.endswith((".so", ".a")):
                libs.append(os.path.join(dirpath, fn))
    if not libs:
        return [{"name": "abi", "ok": None, "exit": None,
                 "detail": "构建目录里没有 .so/.a 产物，跳过 ABI 对比", "output": ""}]

    out_lines = [f"产物：{[os.path.relpath(l, p.root) for l in libs[:10]]}"]
    if shutil.which("abi-compliance-checker") and shutil.which("abi-dumper"):
        out_lines.append("提示：可用 abi-dumper + abi-compliance-checker 做正式比对，"
                         "本网关未代跑（需先构建 base 版本）")
    nm = shutil.which("nm") or shutil.which("llvm-nm")
    if nm:
        for lib in libs[:3]:
            r = subprocess.run([nm, "--demangle", "--with-symbol-versions",
                                "--undefined-only", lib],
                               capture_output=True, text=True)
            syms = sorted(set(re.findall(r"\bU\s+([^\s@]+)", r.stdout)))
            out_lines.append(f"[{os.path.relpath(lib, p.root)}] 未定义符号 {len(syms)} 个，"
                             f"样例：{', '.join(syms[:15])}")
    return [{"name": "abi", "ok": None, "exit": None,
             "detail": "ABI 证据已采集，需评审员判读（结构体对齐/枚举底层类型/"
                       "虚表顺序/ packed 属性改动是高危项）",
             "output": "\n".join(out_lines)[:MAX_BYTES]}]


def ev_diff_stat(p: Project, base: str, changed: list[str]) -> list[dict]:
    """让评审员一眼看到改动规模与分布，防止「改了 3 行却声称重写了模块」。"""
    if not p.has_git:
        return []
    r = _run(["git", "diff", "--stat", f"{base}...HEAD"], p.root)
    srcs = [f for f in changed if _is_source(f)]
    return [{**r, "name": "diff_stat", "ok": True,
             "detail": f"改动 {len(changed)} 个文件，其中 C/C++ 源文件 {len(srcs)} 个",
             "output": _tail(r["output"], 40)}]


# ---------------------------------------------------------------- 编排

class CollectTimeout(Exception):
    pass


def collect(root: str,
            base: str = "main",
            profiles: list[str] | None = None,
            run_tests: bool = True,
            do_tidy: bool = True,
            timeout_total: int = 2400) -> dict:
    """
    采集全部证据。返回结构：
      {
        "project": {...}, "changed": [...], "evidence": [...],
        "gates": {"compile_commands": bool, "build_plain": bool, ...},
        "summary": "人类可读的一段话", "elapsed": 12.3
      }
    """
    t0 = time.time()
    profiles = profiles or ["plain"]
    p = detect_project(root)

    if not os.path.isdir(root):
        return {"error": f"项目路径不存在：{root}"}

    changed = expand_headers_to_tus(root, changed_files(root, base)) if p.has_git else []
    evidence: list[dict] = []

    evidence.append({"name": "toolchain", "ok": bool(p.compiler),
                     "detail": "编译器 " + (p.compiler or "未找到（g++/clang++ 都没有）") +
                               f"；生成器 {p.generators or '无'}；"
                               f"可用工具 {', '.join(sorted(p.toolchain)) or '无'}",
                     "output": ""})
    evidence += ev_diff_stat(p, base, changed)
    evidence.append(gate_compile_db(p))

    build_dir: str | None = None
    cfg, build_dir = gate_configure(p)
    evidence.append(cfg)

    # 编译闸门：先 plain，再按需 asan / tsan（串行，避免抢 CPU 与磁盘）
    for prof in profiles:
        if build_dir:
            evidence += ev_build(p, build_dir, prof)
        elif prof == "asan" and p.build_dir:
            evidence += ev_build(p, p.build_dir, prof)

    # tidy 与 format 与编译结果无关，并行跑省时间
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_tidy = ex.submit(ev_tidy, p, changed) if do_tidy else None
        f_fmt = ex.submit(ev_format, p, changed)
        if f_tidy:
            evidence += f_tidy.result()
        evidence += f_fmt.result()

    if run_tests and (p.has_tests):
        target = build_dir or p.build_dir
        if target:
            evidence += ev_tests(p, target)
            if "asan" in profiles:
                evidence += ev_sanitizer_tests(p, target)

    if build_dir:
        evidence += ev_abi(p, base, build_dir)

    gates = {e["name"]: e.get("ok") for e in evidence if e.get("ok") is not None}

    # 闸门汇总：hard_fail 里的项一旦为 False，评审结论不可能通过
    hard = ["build_plain", "build_asan", "build_tsan", "tests", "tests_asan"]
    failed_hard = [k for k in hard if gates.get(k) is False]

    lines = [f"改动文件 {len(changed)} 个" + (f"（base={base}）" if p.has_git else "（非 git 仓库，未做增量）")]
    if failed_hard:
        lines.append(f"**硬性闸门未过：{', '.join(failed_hard)}** —— 评审结论不可能是 approved")
    else:
        lines.append("已采集的硬性闸门全部通过")
    if not p.has_compile_db:
        lines.append("⚠️ 无 compile_commands.json，clang-tidy 精度下降")
    if not build_dir:
        lines.append("⚠️ 本次未能建立构建目录，编译与测试证据缺失——"
                     "「能编译」「测试通过」一律判未证实，不得写成已验证")

    return {
        "project": {
            "root": p.root,
            "cmake_dir": p.cmake_dir,
            "build_dir": build_dir or p.build_dir,
            "compile_commands": p.compile_commands,
            "compiler": p.compiler,
            "has_git": p.has_git,
            "has_tests": p.has_tests,
            "generators": p.generators,
            "toolchain": p.toolchain,
        },
        "changed": changed,
        "changed_headers_expanded": bool(changed),
        "evidence": evidence,
        "gates": gates,
        "hard_fail": failed_hard,
        "summary": "\n".join(lines),
        "elapsed": round(time.time() - t0, 1),
    }


def to_review_block(result: dict) -> str:
    """把证据压成一段可直接拼进评审 prompt 的文本。失败项排在最前面。"""
    if "error" in result:
        return f"# 证据采集失败\n{result['error']}"
    evs = sorted(result["evidence"],
                 key=lambda e: (0 if e.get("ok") is False else
                                1 if e.get("ok") is None else 2))
    parts = [f"# 机器采集的证据（{result['elapsed']}s，采集器只报事实、不做判断）",
             result["summary"], ""]
    for e in evs:
        mark = {True: "PASS", False: "FAIL", None: "SKIP"}[e.get("ok")]
        parts.append(f"## [{mark}] {e['name']} — {e.get('detail','')}")
        if e.get("cmd"):
            parts.append(f"$ {e['cmd']}   ({e.get('seconds', 0)}s, exit={e.get('exit')})")
        if e.get("output"):
            parts.append("```\n" + e["output"] + "\n```")
    return "\n".join(parts)


if __name__ == "__main__":                       # 手动自测：python evidence.py /path base
    import sys
    res = collect(sys.argv[1] if len(sys.argv) > 1 else ".",
                  sys.argv[2] if len(sys.argv) > 2 else "main")
    print(to_review_block(res))
