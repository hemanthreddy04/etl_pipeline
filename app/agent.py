"""The built-in agent: Claude with the control plane's tools. It runs only when ANTHROPIC_API_KEY is set.

Read-only tools run straight away. A tool that changes something stops the loop and waits for the person
in the chat to approve or reject it.
"""
import json
import threading

import requests

from . import tools
from .config import settings

API = "https://api.anthropic.com/v1/messages"
SYSTEM = """You operate a data platform control plane that builds Bronze, Silver and Gold tables (the medallion pattern).
Use the tools to look before you answer: list connections, pipelines, tables, checks, runs and logs.
Rules:
- Bronze keeps source data as text. Silver casts, validates, deduplicates and merges. Gold is business-ready.
- Before creating a pipeline, check which connections exist, and look at the source files or table so the column list is real.
- Never invent column names. If you cannot see the source, ask the person for the columns.
- Tools that change something need the person's approval. Say plainly what you are about to change and why.
- When a run failed, read its status and logs, explain the cause in plain words, and say what would fix it.
Keep answers short and concrete."""
_sessions, _lock = {}, threading.Lock()


def _session(sid):
    with _lock:
        return _sessions.setdefault(sid, {"messages": [], "pending": None})


def reset(sid):
    with _lock:
        _sessions.pop(sid, None)


def _ask(messages):
    r = requests.post(API, timeout=120, headers={"x-api-key": settings.anthropic_api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                      json={"model": settings.agent_model, "max_tokens": 2000, "system": SYSTEM, "messages": messages,
                            "tools": [{"name": t["name"], "description": t["desc"], "input_schema": t["schema"]} for t in tools.TOOLS]})
    if not r.ok:
        try:
            msg = r.json()["error"]["message"]
        except Exception:
            msg = r.text[:300]
        raise RuntimeError(f"The model API answered {r.status_code}: {msg}")
    return r.json()


def _loop(store, s, events):
    """Call the model until it answers in text or asks for a change that needs approval."""
    for _ in range(12):
        reply = _ask(s["messages"])
        s["messages"].append({"role": "assistant", "content": reply["content"]})
        for block in reply["content"]:
            if block["type"] == "text" and block["text"].strip():
                events.append({"role": "agent", "text": block["text"]})
        calls = [b for b in reply["content"] if b["type"] == "tool_use"]
        if reply.get("stop_reason") != "tool_use" or not calls:
            return
        results = []
        for i, b in enumerate(calls):
            tool = tools.BY_NAME.get(b["name"])
            if tool and tools.needs_approval(store, tool):
                # park this call; anything after it in the same turn is answered as "not run yet"
                s["pending"] = {"call": b, "done": results, "rest": calls[i + 1:]}
                events.append({"role": "approval", "tool": b["name"], "args": b["input"]})
                return
            results.append(_execute(store, b, events))
        s["messages"].append({"role": "user", "content": results})
    events.append({"role": "agent", "text": "I stopped after many steps. Tell me how to continue."})


def _execute(store, block, events, approved=False):
    try:
        out = tools.call(store, block["name"], block["input"], "agent", approved=approved)
        err = False
    except Exception as e:
        out, err = {"error": str(e)}, True
    events.append({"role": "tool", "tool": block["name"], "args": block["input"], "result": out, "ok": not err})
    return {"type": "tool_result", "tool_use_id": block["id"], "content": json.dumps(out, default=str)[:20000], "is_error": err}


def chat(store, sid, text):
    if not settings.anthropic_api_key:
        raise ValueError("The agent is off. Set ANTHROPIC_API_KEY on the service to switch it on.")
    s, events = _session(sid), []
    if s["pending"]:
        raise ValueError("A change is waiting for your approval. Approve or reject it first.")
    s["messages"].append({"role": "user", "content": text})
    try:
        _loop(store, s, events)
    except Exception as e:
        s["messages"].pop() if s["messages"] and s["messages"][-1]["role"] == "user" and isinstance(s["messages"][-1]["content"], str) else None
        raise
    return events


def approve(store, sid, ok):
    s, events = _session(sid), []
    p = s["pending"]
    if not p:
        raise ValueError("Nothing is waiting for approval")
    s["pending"] = None
    results = list(p["done"])
    if ok:
        results.append(_execute(store, p["call"], events, approved=True))
    else:
        results.append({"type": "tool_result", "tool_use_id": p["call"]["id"], "content": "The person rejected this change.", "is_error": True})
        events.append({"role": "tool", "tool": p["call"]["name"], "args": p["call"]["input"], "result": {"rejected": True}, "ok": False})
    for b in p["rest"]:
        results.append({"type": "tool_result", "tool_use_id": b["id"], "content": "Not run: an earlier change in this step was waiting for approval. Ask again if still needed.", "is_error": True})
    s["messages"].append({"role": "user", "content": results})
    _loop(store, s, events)
    return events
