#!/usr/bin/env python3
"""Incrementally export Qoder (intl or CN) chat sessions to TXT.

Reads the Electron app's ``main.sqlite`` (``chat_sessions`` +
``chat_session_messages``) from
``%APPDATA%/com.qoder.app.stable`` (international) or
``%APPDATA%/com.qodercn.app.stable`` (CN). Message payloads are JSON with
typed parts: ``text``, ``thinking`` (reasoning) and ``tool``.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

_TOOLS = Path(__file__).resolve().parent
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))

from chat_export_common import (
    atomic_write_json,
    atomic_write_text,
    clip_text,
    content_to_text,
    empty_counts,
    emit_block,
    epoch_to_iso,
    extract_user_query,
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
    stage_wsl_sqlite,
    write_manifest,
)

EDITIONS: dict[str, dict[str, str]] = {
    "intl": {
        "db": "AppData/Roaming/com.qoder.app.stable/main.sqlite",
        "output_dir": "qoder_chat_live_exports",
        "name": "Qoder",
        "header": "Local Qoder (international) chat export",
    },
    "cn": {
        "db": "AppData/Roaming/com.qodercn.app.stable/main.sqlite",
        "output_dir": "qoder_cn_chat_live_exports",
        "name": "Qoder CN",
        "header": "Local Qoder CN chat export",
    },
}


def parse_args() -> argparse.Namespace:
    home = Path.home()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--edition", choices=sorted(EDITIONS), default="intl")
    parser.add_argument("--db", help="Override the main.sqlite path")
    parser.add_argument("--output-dir", help="Override the export output directory")
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--log-file")
    parser.add_argument("--status-file")
    parser.add_argument("--include-reasoning", action="store_true", default=True)
    parser.add_argument("--no-reasoning", action="store_true")
    args = parser.parse_args()
    edition = EDITIONS[args.edition]
    args.db_path = Path(args.db).expanduser() if args.db else home / edition["db"]
    args.output_path = (
        Path(args.output_dir).expanduser()
        if args.output_dir
        else home / edition["output_dir"]
    )
    args.display_name = edition["name"]
    args.header_label = edition["header"]
    return args


def connect_ro(path: Path) -> sqlite3.Connection | None:
    """Open a db read-only; fall back to a staged local copy when locked."""
    try:
        uri = f"file:{path.as_posix()}?mode=ro"
        con = sqlite3.connect(uri, uri=True, timeout=2.0)
        con.row_factory = sqlite3.Row
        con.execute("SELECT 1").fetchone()
        return con
    except sqlite3.Error:
        staged = stage_wsl_sqlite(path)
        if staged is None:
            return None
        con = sqlite3.connect(f"file:{staged.as_posix()}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        return con


def parse_json(value: Any) -> Any:
    if value is None:
        return {}
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}


def pretty(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, indent=2)
    except TypeError:
        return str(value)


def table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f"PRAGMA table_info({table})")}


def load_sessions(con: sqlite3.Connection) -> list[dict[str, Any]]:
    cols = table_columns(con, "chat_sessions")
    if "session_id" not in cols:
        return []
    wanted = [
        "session_id",
        "title",
        "cwd",
        "model",
        "conversation_mode",
        "product_mode",
        "created_at",
        "updated_at",
    ]
    select_cols = [c for c in wanted if c in cols]
    where = "WHERE deleted_at IS NULL" if "deleted_at" in cols else ""
    rows = con.execute(
        f"""
        SELECT {", ".join(select_cols)}
        FROM chat_sessions
        {where}
        ORDER BY {"updated_at DESC, " if "updated_at" in cols else ""}
                 {"created_at DESC" if "created_at" in cols else "session_id"}
        """
    ).fetchall()
    sessions = []
    for row in rows:
        sessions.append(
            {
                "id": str(row["session_id"]),
                "directory": row["cwd"] if "cwd" in cols else "",
                "title": (row["title"] or "").strip() if "title" in cols else "",
                "model": row["model"] or "" if "model" in cols else "",
                "created": epoch_to_iso(row["created_at"] if "created_at" in cols else None),
                "updated": epoch_to_iso(row["updated_at"] if "updated_at" in cols else None),
                "time_updated": row["updated_at"] if "updated_at" in cols else None,
                "time_created": row["created_at"] if "created_at" in cols else None,
            }
        )
    return sessions


def first_user_title(con: sqlite3.Connection, session_id: str) -> str:
    try:
        rows = con.execute(
            """
            SELECT payload_json FROM chat_session_messages
            WHERE session_id = ? AND role = 'user'
            ORDER BY created_at, sequence, message_id
            """,
            (session_id,),
        ).fetchall()
    except sqlite3.Error:
        return ""
    for row in rows:
        payload = parse_json(row["payload_json"])
        for part in payload.get("parts") or []:
            if not isinstance(part, dict) or part.get("type") != "text":
                continue
            text = extract_user_query(str(part.get("text") or content_to_text(part)))
            if should_skip_user_text(text):
                continue
            if text.strip():
                return one_line(text, 80)
    return ""


def render_tool_part(lines: list[str], part: dict[str, Any], counts: dict[str, int], ts: str) -> None:
    info = part.get("tool") if isinstance(part.get("tool"), dict) else part
    name = info.get("name") or info.get("tool") or "tool"
    status = info.get("status") or ""
    inp = info.get("input")
    out = info.get("response")
    if out is None:
        out = info.get("output") or info.get("result")
    error = info.get("error")
    call_lines = [f"Name: {name}"]
    if status:
        call_lines.append(f"Status: {status}")
    if inp is not None:
        call_lines.extend(["Input:", clip_text(pretty(inp), 40_000)])
    emit_block(lines, "TOOL CALL", "\n".join(call_lines), ts)
    counts["tool_call"] += 1
    if out is not None or error:
        out_lines = []
        if error:
            out_lines.extend(["Error:", clip_text(pretty(error), 20_000)])
        if out is not None:
            out_lines.extend(["Output:", clip_text(pretty(out), 80_000)])
        emit_block(lines, "TOOL OUTPUT", "\n".join(out_lines), ts)
        counts["tool_output"] += 1


def export_session(
    con: sqlite3.Connection,
    session: dict[str, Any],
    title: str,
    output_path: Path,
    *,
    include_reasoning: bool,
    header_label: str,
) -> dict[str, Any]:
    session_id = session["id"]
    counts = empty_counts()
    lines: list[str] = [
        header_label,
        f"Title: {title}",
        f"Session ID: {session_id}",
    ]
    if session.get("created"):
        lines.append(f"Created: {session['created']}")
    if session.get("updated"):
        lines.append(f"Updated: {session['updated']}")
    if session.get("directory"):
        lines.append(f"CWD: {session['directory']}")
    if session.get("model"):
        lines.append(f"Model: {session['model']}")
    lines.extend(
        [
            f"Source: {session.get('source_db', '')}",
            f"Exported At: {now_iso()}",
        ]
    )

    try:
        messages = con.execute(
            """
            SELECT message_id, payload_json, role, created_at
            FROM chat_session_messages
            WHERE session_id = ?
            ORDER BY created_at, sequence, message_id
            """,
            (session_id,),
        ).fetchall()
    except sqlite3.Error as exc:
        emit_block(lines, "WARNING", f"Could not read messages: {exc}")
        messages = []

    for msg_row in messages:
        role = str(msg_row["role"] or "unknown").lower()
        ts = epoch_to_iso(msg_row["created_at"])
        payload = parse_json(msg_row["payload_json"])
        parts = [p for p in (payload.get("parts") or []) if isinstance(p, dict)]

        text_chunks: list[str] = []
        for part in parts:
            ptype = str(part.get("type") or "")
            if ptype == "text":
                chunk = str(part.get("text") or content_to_text(part) or "").strip()
                if chunk:
                    text_chunks.append(chunk)
            elif ptype == "thinking":
                if not include_reasoning:
                    continue
                body = str(part.get("text") or "").strip()
                if body:
                    emit_block(lines, "REASONING", clip_text(body), ts)
                    counts["reasoning"] += 1
            elif ptype == "tool":
                render_tool_part(lines, part, counts, ts)
            elif ptype in ("image", "input_image"):
                emit_block(lines, "IMAGE", "[image attachment]", ts)
            # other internal part types are skipped

        if not text_chunks:
            continue
        body = "\n\n".join(text_chunks)
        if role == "user":
            body = extract_user_query(body)
            if should_skip_user_text(body):
                continue
            emit_block(lines, "USER", body, ts)
            counts["user"] += 1
        elif role == "assistant":
            emit_block(lines, "ASSISTANT", body, ts)
            counts["assistant"] += 1
        elif role == "system":
            counts["system"] += 1
        else:
            emit_block(lines, role.upper(), body, ts)

    output_text = scrub_internal_lines("\n".join(lines) + "\n")
    atomic_write_text(output_path, output_text)
    return {
        "session_id": session_id,
        "source_id": session_id,
        "title": title,
        "kind": "session",
        "source": f"{session.get('source_db', '')}#session:{session_id}",
        "output": str(output_path),
        "created": session.get("created", ""),
        "updated": session.get("updated", ""),
        "cwd": session.get("directory", ""),
        "model": session.get("model", ""),
        "counts": counts,
        "bytes": output_path.stat().st_size,
        "time_updated": session.get("time_updated"),
    }


def session_title(con: sqlite3.Connection, session: dict[str, Any]) -> str:
    title = session.get("title") or ""
    if title and title.lower() not in {"untitled", "new session", "new chat"}:
        return one_line(title, 80)
    guessed = first_user_title(con, session["id"])
    if guessed:
        return guessed
    return session["id"]


def scan_once(db: Path, output_dir: Path, *, include_reasoning: bool, header_label: str) -> tuple[int, int, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = output_dir / ".export_state.json"
    state_obj = load_json(state_path, {"sources": {}})
    old_sources: dict[str, Any] = state_obj.get("sources") or {}
    new_sources: dict[str, Any] = {}
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    changed = 0

    fingerprint = {
        str(p): list(file_fingerprint(p))
        for p in (db, Path(str(db) + "-wal"), Path(str(db) + "-shm"))
    }
    if not db.exists():
        removed = prune_removed_sources(old_sources, seen, retained_sources=new_sources, retained_records=records)
        write_manifest(output_dir, f"{header_label} (missing {db})", records, changed, removed)
        atomic_write_json(
            state_path,
            {"updated_at": now_iso(), "db": str(db), "missing": True, "sources": new_sources},
        )
        return 0, 0, removed

    con = connect_ro(db)
    if con is None:
        emit_runtime_error(output_dir, db)
        return len(old_sources), 0, 0
    try:
        sessions = load_sessions(con)
        for ordinal, session in enumerate(sessions, 1):
            session["source_db"] = str(db)
            source_key = f"qoder:{session['id']}"
            seen.add(source_key)
            title = session_title(con, session)
            prefix = first_iso_timestamp_prefix(session.get("created"), ordinal)
            stem = f"{prefix}__{session['id']}__"
            output_path = filesystem_safe_output_path(output_dir, stem, title)
            old = old_sources.get(source_key, {})
            must_export = (
                not old
                or old.get("time_updated") != session.get("time_updated")
                or old.get("title") != title
                or old.get("output") != str(output_path)
                or not Path(old.get("output") or "").exists()
                or not output_path.exists()
            )
            if must_export:
                record = export_session(
                    con, session, title, output_path,
                    include_reasoning=include_reasoning,
                    header_label=header_label,
                )
                reuse_or_replace_output(old.get("output"), output_path)
                changed += 1
            else:
                record = {
                    "session_id": session["id"],
                    "source_id": session["id"],
                    "title": title,
                    "kind": "session",
                    "source": f"{db}#session:{session['id']}",
                    "output": str(output_path),
                    "created": session.get("created") or old.get("created", ""),
                    "updated": session.get("updated") or old.get("updated", ""),
                    "cwd": session.get("directory") or old.get("cwd", ""),
                    "model": session.get("model") or old.get("model", ""),
                    "counts": old.get("counts") or empty_counts(),
                    "bytes": old.get(
                        "bytes",
                        output_path.stat().st_size if output_path.exists() else 0,
                    ),
                    "time_updated": session.get("time_updated"),
                }
            new_sources[source_key] = record
            records.append(record)
    finally:
        con.close()

    removed = prune_removed_sources(old_sources, seen, retained_sources=new_sources, retained_records=records)
    write_manifest(output_dir, f"{header_label} {db}", records, changed, removed)
    atomic_write_json(
        state_path,
        {
            "updated_at": now_iso(),
            "db": str(db),
            "fingerprint": fingerprint,
            "sources": new_sources,
        },
    )
    return len(records), changed, removed


def emit_runtime_error(output_dir: Path, db: Path) -> None:
    """Keep the old state when the db cannot be opened at all."""
    append_note = output_dir / "watcher.log"
    try:
        from chat_export_common import append_log_line

        append_log_line(append_note, f"{now_iso()} ERROR: could not open {db}")
    except Exception:
        pass


def main() -> int:
    args = parse_args()
    db = args.db_path
    output_dir = args.output_path
    include_reasoning = bool(args.include_reasoning) and not args.no_reasoning
    return run_watcher_loop(
        once=args.once,
        interval=args.interval,
        log_file=Path(args.log_file).expanduser() if args.log_file else None,
        status_file=Path(args.status_file).expanduser() if args.status_file else None,
        extra_status={"db": str(db), "output_dir": str(output_dir), "edition": args.edition},
        scan_fn=lambda: scan_once(
            db, output_dir, include_reasoning=include_reasoning, header_label=args.header_label
        ),
        quiet=args.quiet,
        label=f"qoder-{args.edition}",
    )


if __name__ == "__main__":
    raise SystemExit(main())
