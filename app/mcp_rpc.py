"""A small Model Context Protocol endpoint (Streamable HTTP, JSON responses) that exposes the tools in tools.py."""
import json

from . import __version__, tools

VERSIONS = ("2025-06-18", "2025-03-26", "2024-11-05")


def handle(store, msg, by="mcp client"):
    """Handle one JSON-RPC message. Returns a response dict, or None for a notification."""
    method, mid = msg.get("method"), msg.get("id")
    if mid is None:
        return None
    params = msg.get("params") or {}
    try:
        if method == "initialize":
            wanted = params.get("protocolVersion")
            result = {"protocolVersion": wanted if wanted in VERSIONS else VERSIONS[0], "capabilities": {"tools": {"listChanged": False}},
                      "serverInfo": {"name": "medallion-control-plane", "version": __version__},
                      "instructions": "Tools to inspect and operate Bronze, Silver and Gold pipelines. Changes may wait for a person's approval."}
        elif method == "ping":
            result = {}
        elif method == "tools/list":
            result = {"tools": [{"name": t["name"], "description": t["desc"] + (" Changes data: may need approval." if t["write"] else ""),
                                 "inputSchema": t["schema"]} for t in tools.TOOLS]}
        elif method == "tools/call":
            try:
                out = tools.call(store, params.get("name"), params.get("arguments") or {}, by)
                result = {"content": [{"type": "text", "text": json.dumps(out, default=str)}], "isError": False}
            except Exception as e:
                result = {"content": [{"type": "text", "text": str(e)}], "isError": True}
        else:
            return {"jsonrpc": "2.0", "id": mid, "error": {"code": -32601, "message": f"Method not found: {method}"}}
        return {"jsonrpc": "2.0", "id": mid, "result": result}
    except Exception as e:
        return {"jsonrpc": "2.0", "id": mid, "error": {"code": -32603, "message": str(e)}}
