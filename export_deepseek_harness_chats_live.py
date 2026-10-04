#!/usr/bin/env python3
"""Export DeepSeek Harness sessions (~/.dsh/sessions) to TXT.

Session logs are append-only streams of zstd-compressed JSONL events
(``session.v4.jsonl.zstd``): every appended event is its own zstd frame, so a
file is a concatenation of frames. Decoding requires either the ``zstandard``
pip package (preferred) or a ``node`` executable with zstd support (fallback).
"""

from __future__ import annotations

import argparse
import io
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

_TOOLS = Path(__file__).resolve().parent
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))

from chat_export_common import (
    SKIP_USER_PREFIXES,
    atomic_write_json,
    atomic_write_text,
    clip_text,
    content_to_text,
    empty_counts,
    emit_block,
    epoch_to_iso,
    file_fingerprint,
    filesystem_safe_output_path,
    first_iso_timestamp_prefix,
    load_json,
    now_iso,
    one_line,
    prune_removed_sources,
    reuse_or_replace_output,
    run_watcher_loop,
    scrub_internal_lines,
    should_skip_user_text,
    wsl_agent_homes,
    write_manifest,
)

# DeepSeek Harness injects runtime-context snapshots as user messages; they are
# machine state, not conversation, so skip them in addition to the shared set.
DSH_SKIP_USER_PREFIXES = SKIP_USER_PREFIXES + ("Current runtime context.",)

SESSION_FILE_RE = re.compile(r"^session\.v\d+\.jsonl(?:\.zstd)?$")

# Fallback decoder: walk concatenated zstd frames with node (zlib zstd support
# exists in node >= 23). Reads the file itself, writes raw bytes to stdout.
_NODE_DECODE_SCRIPT = r"""
const fs = require('fs');
const zlib = require('node:zlib');
const file = process.argv[1];
if (!file) { process.stderr.write('missing file argument\n'); process.exit(2); }
const buf = fs.readFileSync(file);
const offsets = [];
for (let i = 0; i + 4 <= buf.length; i++) {
  if (buf[i] === 0x28 && buf[i+1] === 0xb5 && buf[i+2] === 0x2f && buf[i+3] === 0xfd) offsets.push(i);
}
offsets.push(buf.length);
let out = [];
for (let i = 0; i < offsets.length - 1; i++) {
  for (let j = i + 1; j < Math.min(i + 6, offsets.length); j++) {
    try {
      out.push(zlib.zstdDecompressSync(buf.subarray(offsets[i], offsets[j])));
      i = j - 1;
      break;
    } catch (e) { /* try the next boundary */ }
  }
}
process.stdout.write(Buffer.concat(out));
"""


def parse_args() -> argparse.Namespace:
    home = Path.home()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--home", default=str(home / ".dsh"))
    parser.add_argument("--output-dir", default=str(home / "deepseek_harness_chat_live_exports"))
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--log-file")
    parser.add_argument("--status-file")
    return parser.parse_args()


class ZstdUnavailable(RuntimeError):
    pass


def _decode_zstd_via_module(data: bytes) -> bytes:
    import zstandard  # type: ignore

    dctx = zstandard.ZstdDecompressor()
    reader = dctx.stream_reader(io.BytesIO(data), read_across_frames=True)
    chunks: list[bytes] = []
    while True:
        try:
            chunk = reader.read(1 << 20)
        except Exception:
            # A truncated trailing frame happens while DeepSeek Harness is
            # mid-append; keep everything decoded so far.
            break
        if not chunk:
            break
        chunks.append(chunk)
    return b"".join(chunks)


def _decode_zstd_via_node(path: Path) -> bytes:
    try:
        proc = subprocess.run(
            ["node", "-e", _NODE_DECODE_SCRIPT, str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except (OSError, ValueError):
        raise ZstdUnavailable("node executable not found")
    if proc.returncode != 0 or not proc.stdout:
        raise ZstdUnavailable(f"node decode failed: {proc.stderr.decode('utf-8', 'replace').strip()}")
    return proc.stdout


def read_session_bytes(path: Path) -> bytes:
    """Decode the concatenated zstd frames of one session log."""
    try:
        return _decode_zstd_via_module(path.read_bytes())
    except ImportError:
        pass
    except ZstdUnavailable:
        raise
    except Exception:
        # Corrupt read through the module: fall back to node frame walking.
        pass
    return _decode_zstd_via_node(path)


def pretty(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, indent=2)
    except TypeError:
        return str(value)


def block_text(blocks: Any) -> str:
    if blocks is None:
        return ""
    if not isinstance(blocks, list):
        return content_to_text(blocks)
    chunks: list[str] = []
    for item in blocks:
        if isinstance(item, str):
            chunks.append(item)
        elif isinstance(item, dict) and str(item.get("type") or "") in ("text", "input_text", "output_text"):
            chunks.append(str(item.get("text") or ""))
        elif isinstance(item, dict):
            chunks.append(pretty(item))
        else:
            chunks.append(str(item))
    return "\n\n".join(c for c in chunks if c and c.strip()).strip()


def parse_session_file(path: Path, counts: dict[str, int]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    meta: dict[str, Any] = {}
    events: list[dict[str, Any]] = []
    try:
        raw = read_session_bytes(path)
    except (ZstdUnavailable, OSError) as exc:
        raise
    for line in raw.decode("utf-8", "replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            counts["parse_error"] += 1
            continue
        if not isinstance(obj, dict):
            continue
        otype = str(obj.get("type") or "")
        if otype == "session" and not meta:
            meta = obj
        elif otype:
            events.append(obj)
    return meta, events


def render_events(lines: list[str], counts: dict[str, int], events: list[dict[str, Any]]) -> dict[str, Any]:
    info: dict[str, Any] = {
        "title": "",
        "title_source": "",
        "model": "",
        "sandbox_mode": "",
        "permission_preset": "",
        "approval_policy": "",
        "turns": 0,
    }
    pending_approvals: dict[str, dict[str, Any]] = {}

    def flush_approval(approval_id: str, outcome: str = "") -> None:
        item = pending_approvals.pop(approval_id, None)
        if item is None:
            return
        body_lines = [f"Tool: {item.get('tool', 'unknown')}"]
        if outcome:
            body_lines.append(f"Outcome: {outcome}")
        if item.get("reason"):
            body_lines.append(f"Reason: {item['reason']}")
        emit_block(lines, "APPROVAL", "\n".join(body_lines), item.get("time") or None)

    for obj in events:
        otype = str(obj.get("type") or "")
        data = obj.get("data")
        if not isinstance(data, dict):
            data = {}
        ts = epoch_to_iso(obj.get("time"))

        if otype == "turn/start":
            info["turns"] = max(info["turns"], int(data.get("turn") or 0))
            turn = int(data.get("turn") or 0)
            if turn > 1:
                lines.extend(["", "=" * 88, f"TURN {turn}", "-" * 88])
        elif otype == "permission/preset":
            info["permission_preset"] = info["permission_preset"] or str(data.get("preset") or "")
        elif otype == "sandbox/mode":
            info["sandbox_mode"] = info["sandbox_mode"] or str(data.get("mode") or "")
        elif otype == "approval/policy":
            info["approval_policy"] = info["approval_policy"] or str(data.get("policy") or "")
        elif otype == "session/title":
            title = one_line(str(data.get("title") or ""), 120)
            if title:
                info["title"] = title
                source = data.get("source")
                info["title_source"] = str(source.get("kind") or "") if isinstance(source, dict) else ""
        elif otype == "model/selection":
            provider = str(data.get("provider") or "")
            model = str(data.get("model") or "")
            if provider or model:
                info["model"] = f"{provider}/{model}".strip("/")
        elif otype == "system/message":
            message = data.get("message")
            body = ""
            if isinstance(message, dict):
                # The first system message is the (long) agent prompt; keep a
                # short prefix so per-session boilerplate does not dominate.
                body = clip_text(block_text(message.get("content")), 2_000)
                if len(body) >= 2_000:
                    body += "\n[system prompt clipped]"
            if body:
                emit_block(lines, "SYSTEM", body, ts)
                counts["system"] += 1
        elif otype == "user/message":
            body = block_text(data.get("content"))
            if not should_skip_dsh_user_text(body):
                emit_block(lines, "USER", body, ts)
                counts["user"] += 1
        elif otype == "assistant/message":
            message = data.get("message")
            if not isinstance(message, dict):
                continue
            source = message.get("source")
            if isinstance(source, dict) and not info["model"]:
                provider = str(source.get("provider") or "")
                model = str(source.get("model") or "")
                if provider or model:
                    info["model"] = f"{provider}/{model}".strip("/")
            for item in message.get("content") or []:
                if not isinstance(item, dict):
                    continue
                itype = str(item.get("type") or "")
                if itype == "reasoning":
                    body = str(item.get("text") or "").strip()
                    if body:
                        emit_block(lines, "REASONING", clip_text(body), ts)
                        counts["reasoning"] += 1
                elif itype == "text":
                    body = str(item.get("text") or "").strip()
                    if body:
                        emit_block(lines, "ASSISTANT", clip_text(body), ts)
                        counts["assistant"] += 1
                # tool-call blocks are rendered from the tool/call events
        elif otype == "tool/call":
            name = str(data.get("name") or "tool")
            args_raw = data.get("arguments")
            if isinstance(args_raw, str):
                args_text = args_raw
                try:
                    args_text = json.dumps(json.loads(args_raw), ensure_ascii=False, indent=2)
                except (json.JSONDecodeError, TypeError):
                    pass
            else:
                args_text = pretty(args_raw)
            emit_block(lines, "TOOL CALL", f"Name: {name}\n{clip_text(args_text, 20_000)}", ts)
            counts["tool_call"] += 1
        elif otype == "tool/result":
            message = data.get("message")
            body = ""
            if isinstance(message, dict):
                body = block_text(message.get("content"))
            emit_block(lines, "TOOL OUTPUT", clip_text(body, 40_000), ts)
            counts["tool_output"] += 1
        elif otype == "todo/write":
            todos = data.get("todos")
            todo_lines = []
            if isinstance(todos, list):
                for todo in todos:
                    if isinstance(todo, dict):
                        todo_lines.append(
                            f"- [{todo.get('status', '?')}] {todo.get('content', '')}"
                        )
            if todo_lines:
                emit_block(lines, "TODO LIST", "\n".join(todo_lines), ts)
        elif otype == "approval/asked":
            approval_id = str(data.get("id") or "")
            pending_approvals[approval_id] = {
                "tool": str(data.get("toolName") or ""),
                "reason": str(data.get("reason") or ""),
                "time": ts,
            }
        elif otype == "approval/decided":
            flush_approval(str(data.get("id") or ""), str(data.get("outcome") or ""))
        elif otype == "deliverables/presented":
            files = data.get("files")
            file_lines = []
            if isinstance(files, list):
                for item in files:
                    if isinstance(item, dict):
                        line = str(item.get("path") or "")
                        desc = str(item.get("description") or "")
                        if desc:
                            line = f"{line}\n    {desc}" if line else desc
                        if line:
                            file_lines.append(line)
            if file_lines:
                emit_block(lines, "DELIVERABLES", "\n".join(file_lines), ts)
        elif otype == "goal/change":
            goal = data.get("goal")
            if isinstance(goal, dict):
                operation = str(data.get("operation") or "")
                objective = one_line(str(goal.get("objective") or ""), 300)
                emit_block(
                    lines,
                    "GOAL",
                    f"{operation}: {objective}".strip(": "),
                    ts,
                )
        elif otype == "subagent/catalog":
            label = str(data.get("label") or "")
            mode = str(data.get("mode") or "")
            if label or mode:
                emit_block(lines, "SUBAGENT", f"{label} ({mode})", ts)
        elif otype == "compaction/summary":
            summary = data.get("summary")
            body = block_text(summary) if isinstance(summary, list) else ""
            if body:
                emit_block(lines, "COMPACTION SUMMARY", clip_text(body, 8_000), ts)

    for approval_id in list(pending_approvals):
        flush_approval(approval_id)
    return info


def should_skip_dsh_user_text(text: str) -> bool:
    stripped = (text or "").lstrip()
    if not stripped:
        return True
    return any(stripped.startswith(prefix) for prefix in DSH_SKIP_USER_PREFIXES)


def first_user_title(events: list[dict[str, Any]]) -> str:
    for obj in events:
        if str(obj.get("type") or "") != "user/message":
            continue
        data = obj.get("data")
        if not isinstance(data, dict):
            continue
        text = block_text(data.get("content"))
        if not should_skip_dsh_user_text(text):
            return one_line(text, 80)
    return ""


def scan_once(home: Path, output_dir: Path) -> tuple[int, int, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = output_dir / ".export_state.json"
    state_obj = load_json(state_path, {"sources": {}})
    old_sources: dict[str, Any] = state_obj.get("sources") or {}
    new_sources: dict[str, Any] = {}
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    changed = 0
    ordinal = 0

    scan_homes: list[tuple[Path, str]] = [(home, "")] + wsl_agent_homes(".dsh")
    for scan_home, home_tag in scan_homes:
        sessions_root = scan_home / "sessions"
        if not sessions_root.is_dir():
            continue
        for path in sorted(sessions_root.rglob("session.v*.jsonl*")):
            if not SESSION_FILE_RE.match(path.name):
                continue
            ordinal += 1
            source_key = str(path)
            seen.add(source_key)
            mtime_ns, size = file_fingerprint(path)
            counts = empty_counts()
            try:
                meta, events = parse_session_file(path, counts)
            except ZstdUnavailable as exc:
                raise RuntimeError(
                    "DeepSeek Harness sessions are zstd-compressed; install the "
                    "decoder with `pip install zstandard` (or make node >= 23 "
                    "available on PATH)."
                ) from exc
            sid = str(meta.get("id") or path.parent.name)
            short_sid = sid
            match = re.search(
                r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                sid,
                re.IGNORECASE,
            )
            if match:
                short_sid = match.group(0)
            created = epoch_to_iso(meta.get("createdAt"))
            cwd = str(meta.get("cwd") or "")
            workspace = path.parent.parent.name if path.parent.parent != sessions_root else ""
            try:
                updated = epoch_to_iso(path.stat().st_mtime)
            except OSError:
                updated = ""
            prefix = first_iso_timestamp_prefix(created or None, ordinal)

            old = old_sources.get(source_key, {})
            must_export = (
                not old
                or old.get("mtime_ns") != mtime_ns
                or old.get("size") != size
                or not Path(old.get("output") or "").exists()
            )
            if must_export:
                lines: list[str] = [
                    "DeepSeek Harness chat export",
                ]
                info = render_events(lines, counts, events)
                title = info.get("title") or first_user_title(events) or f"DeepSeek Harness {short_sid[:16]}"
                stem = (
                    f"{prefix}__{short_sid[:36]}__"
                    if not home_tag
                    else f"{prefix}__{home_tag}_{short_sid[:36]}__"
                )
                output_path = filesystem_safe_output_path(output_dir, stem, title)
                header = [
                    f"Title: {title}",
                    f"Session ID: {sid}",
                ]
                if created:
                    header.append(f"Created: {created}")
                if updated:
                    header.append(f"Updated: {updated}")
                if cwd:
                    header.append(f"CWD: {cwd}")
                if workspace:
                    header.append(f"Workspace: {workspace}")
                preset = str(meta.get("agentPreset") or "")
                if preset:
                    header.append(f"Agent Preset: {preset}")
                depth = meta.get("delegationDepth")
                if isinstance(depth, int) and depth > 0:
                    header.append(f"Subagent Session (delegation depth {depth})")
                if info.get("model"):
                    header.append(f"Model: {info['model']}")
                if info.get("sandbox_mode"):
                    header.append(f"Sandbox: {info['sandbox_mode']}")
                if info.get("approval_policy"):
                    header.append(f"Approval Policy: {info['approval_policy']}")
                header.extend(
                    [
                        f"Source: {path}",
                        f"Exported At: {now_iso()}",
                        "",
                        "Note: reasoning blocks are preserved; tool outputs are "
                        "clipped and sensitive lines scrubbed.",
                    ]
                )
                text = "\n".join([lines[0], *header, *lines[1:]])
                atomic_write_text(output_path, scrub_internal_lines(text + "\n"))
                record = {
                    "session_id": sid,
                    "source_id": short_sid,
                    "title": title,
                    "kind": "session",
                    "source": source_key,
                    "output": str(output_path),
                    "created": created,
                    "updated": updated,
                    "cwd": cwd,
                    "model": info.get("model", ""),
                    "counts": counts,
                    "bytes": output_path.stat().st_size,
                }
                reuse_or_replace_output(old.get("output"), output_path)
                changed += 1
            else:
                record = dict(old)
            record["mtime_ns"] = mtime_ns
            record["size"] = size
            new_sources[source_key] = record
            records.append(record)

    removed = prune_removed_sources(old_sources, seen, retained_sources=new_sources, retained_records=records)
    write_manifest(output_dir, "DeepSeek Harness", records, changed, removed)
    atomic_write_json(
        state_path,
        {
            "updated_at": now_iso(),
            "home": str(home),
            "sources": new_sources,
        },
    )
    return len(records), changed, removed


def main() -> int:
    args = parse_args()
    home = Path(args.home).expanduser()
    output_dir = Path(args.output_dir).expanduser()
    return run_watcher_loop(
        once=args.once,
        interval=args.interval,
        log_file=Path(args.log_file).expanduser() if args.log_file else None,
        status_file=Path(args.status_file).expanduser() if args.status_file else None,
        extra_status={"home": str(home), "output_dir": str(output_dir)},
        scan_fn=lambda: scan_once(home, output_dir),
        quiet=args.quiet,
        label="deepseek-harness",
    )


if __name__ == "__main__":
    raise SystemExit(main())
