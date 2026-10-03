"""HTTP API and static site. Start with:  uvicorn app.main:app --host 0.0.0.0 --port 8080"""
import contextlib
import hmac
import os
import threading

from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.responses import FileResponse, JSONResponse, Response
from starlette.routing import Route

from . import agent, mcp_rpc, runner, service, tools
from .config import settings
from .engines import EngineError
from .store import Store

store = Store()
STATIC = os.path.join(os.path.dirname(__file__), "static")


def authorised(request):
    if not settings.app_token:
        return True
    sent = request.headers.get("authorization", "")
    sent = sent[7:] if sent.lower().startswith("bearer ") else request.headers.get("x-app-token", "")
    return hmac.compare_digest(sent.encode(), settings.app_token.encode())


def api(fn):
    """Wrap a handler: check the token, parse JSON, run in a worker thread, turn errors into messages."""
    async def endpoint(request):
        if not authorised(request):
            return JSONResponse({"error": "This control plane is protected. Enter the access token."}, status_code=401)
        body = {}
        if request.method in ("POST", "PUT", "DELETE"):
            try:
                body = await request.json()
            except Exception:
                body = {}
        try:
            out = await run_in_threadpool(fn, request, body)
        except service.NotFound as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        except (ValueError, EngineError, RuntimeError) as e:
            return JSONResponse({"error": str(e)}, status_code=400)
        if isinstance(out, Response):
            return out
        store.save()
        return JSONResponse(out if out is not None else {"ok": True})
    return endpoint


P = lambda r, k: r.path_params[k]   # noqa: E731

routes = [
    Route("/api/state", api(lambda r, b: service.state_view(store))),
    Route("/api/connections", api(lambda r, b: service.save_connection(store, b, True)), methods=["POST"]),
    Route("/api/connections/{id}", api(lambda r, b: service.save_connection(store, {**b, "id": P(r, "id")}, False)), methods=["PUT"]),
    Route("/api/connections/{id}", api(lambda r, b: service.delete_connection(store, P(r, "id"))), methods=["DELETE"]),
    Route("/api/connections/{id}/test", api(lambda r, b: service.test_connection(store, P(r, "id"))), methods=["POST"]),
    Route("/api/files", api(lambda r, b: service.list_source_files(store, b.get("connection"), b.get("path", ""), b.get("format", "csv"))), methods=["POST"]),
    Route("/api/columns", api(lambda r, b: service.detect_columns(store, b.get("cfg") or {})), methods=["POST"]),
    Route("/api/plan", api(lambda r, b: service.plan_sql(store, cfg=b.get("cfg"), pid=b.get("pipeline"))), methods=["POST"]),
    Route("/api/pipelines", api(lambda r, b: service.deploy_pipeline(store, b.get("cfg") or {})), methods=["POST"]),
    Route("/api/pipelines/{id}", api(lambda r, b: service.delete_pipeline(store, P(r, "id"), bool(b.get("dropTables")))), methods=["DELETE"]),
    Route("/api/pipelines/{id}/run", api(lambda r, b: service.start(store, P(r, "id"), bool(b.get("full")))), methods=["POST"]),
    Route("/api/pipelines/{id}/toggle", api(lambda r, b: service.toggle_pipeline(store, P(r, "id"))), methods=["POST"]),
    Route("/api/pipelines/{id}/schedule", api(lambda r, b: service.set_schedule(store, P(r, "id"), b)), methods=["PUT"]),
    Route("/api/pipelines/{id}/controls", api(lambda r, b: service.set_control(store, P(r, "id"), b.get("key"), b.get("value"))), methods=["PUT"]),
    Route("/api/pipelines/{id}/recommend", api(lambda r, b: service.recommend_rules(store, P(r, "id"), b.get("layer"))), methods=["POST"]),
    Route("/api/runs/{id}/retry", api(lambda r, b: runner.retry_run(store, P(r, "id"))), methods=["POST"]),
    Route("/api/mappings/{table}", api(lambda r, b: service.save_mapping(store, P(r, "table"), b.get("rows") or [])), methods=["PUT"]),
    Route("/api/rules", api(lambda r, b: service.add_rule(store, b)), methods=["POST"]),
    Route("/api/rules/run", api(lambda r, b: service.run_checks(store, b.get("table"))), methods=["POST"]),
    Route("/api/rules/{id}", api(lambda r, b: service.update_rule(store, P(r, "id"), b)), methods=["PUT"]),
    Route("/api/rules/{id}", api(lambda r, b: service.delete_rule(store, P(r, "id"))), methods=["DELETE"]),
    Route("/api/tables/refresh", api(lambda r, b: service.refresh_tables(store)), methods=["POST"]),
    Route("/api/tables/{id}/preview", api(lambda r, b: service.preview_table(store, P(r, "id")))),
    Route("/api/quarantine/{table}/{run}", api(lambda r, b: service.quarantine_rows(store, P(r, "table"), P(r, "run")))),
    Route("/api/quarantine/{table}/{run}", api(lambda r, b: service.discard_quarantine(store, P(r, "table"), P(r, "run"))), methods=["DELETE"]),
    Route("/api/drift", api(lambda r, b: service.drift_action(store, b.get("table"), b.get("col"), b.get("action"))), methods=["POST"]),
    Route("/api/alerts/{id}/ack", api(lambda r, b: _ack(P(r, "id"))), methods=["POST"]),
    Route("/api/compute", api(lambda r, b: {"compute": service.compute_view(store)})),
    Route("/api/compute/{conn}/{id}", api(lambda r, b: service.toggle_compute(store, P(r, "conn"), P(r, "id"), bool(b.get("start")))), methods=["POST"]),
    Route("/api/governance/grants", api(lambda r, b: service.grants(store, bool(b.get("apply")), b.get("roles"))), methods=["POST"]),
    Route("/api/governance/erase", api(lambda r, b: service.erase(store, b.get("column", ""), str(b.get("value", "")), bool(b.get("confirm")))), methods=["POST"]),
    Route("/api/governance/secrets", api(lambda r, b: {"secrets": service.secrets_view(store)})),
    Route("/api/export", api(lambda r, b: Response(service.export_zip(b.get("files") or []), media_type="application/zip",
                                                   headers={"content-disposition": "attachment; filename=medallion-pipelines.zip"})), methods=["POST"]),
    Route("/api/tools", api(lambda r, b: {"tools": tools.describe(store)})),
    Route("/api/tools/{name}", api(lambda r, b: _tool_setting(P(r, "name"), b)), methods=["PUT"]),
    Route("/api/tools/{name}", api(lambda r, b: {"result": tools.call(store, P(r, "name"), b.get("args") or {}, "you", approved=True)}), methods=["POST"]),
    Route("/api/approvals/{id}", api(lambda r, b: tools.decide(store, P(r, "id"), bool(b.get("ok")))), methods=["POST"]),
    Route("/api/agent", api(lambda r, b: {"events": agent.chat(store, b.get("session", "default"), str(b.get("message") or ""))}), methods=["POST"]),
    Route("/api/agent/approve", api(lambda r, b: {"events": agent.approve(store, b.get("session", "default"), bool(b.get("ok")))}), methods=["POST"]),
    Route("/api/agent/reset", api(lambda r, b: agent.reset(b.get("session", "default"))), methods=["POST"]),
    Route("/api/tick", api(lambda r, b: {"started": runner.tick(store)}), methods=["POST", "GET"]),
]


def _ack(aid):
    a = store.find("alerts", aid)
    if a:
        a["ack"] = True
        store.audit("Acknowledged alert", a["title"])


def _tool_setting(name, body):
    if name not in tools.BY_NAME:
        raise service.NotFound("Unknown tool")
    store.data["toolApproval"][name] = bool(body.get("approve"))
    store.audit("Changed tool approval", name, "ask first" if body.get("approve") else "runs without asking")


async def mcp_endpoint(request):
    if not authorised(request):
        return JSONResponse({"error": "unauthorised"}, status_code=401)
    if request.method != "POST":
        return Response(status_code=405, headers={"allow": "POST"})
    try:
        msg = await request.json()
    except Exception:
        return JSONResponse({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}, status_code=400)
    by = request.headers.get("x-client-name", "mcp client")
    if isinstance(msg, list):
        out = [x for x in [await run_in_threadpool(mcp_rpc.handle, store, m, by) for m in msg] if x is not None]
        return JSONResponse(out) if out else Response(status_code=202)
    out = await run_in_threadpool(mcp_rpc.handle, store, msg, by)
    return JSONResponse(out) if out is not None else Response(status_code=202)


async def index(request):
    return FileResponse(os.path.join(STATIC, "index.html"), headers={"cache-control": "no-store"})


async def health(request):
    return JSONResponse({"ok": True})


routes += [Route("/mcp", mcp_endpoint, methods=["GET", "POST", "DELETE"]), Route("/healthz", health), Route("/", index)]


@contextlib.asynccontextmanager
async def lifespan(app):
    if settings.scheduler:
        threading.Thread(target=runner.scheduler_loop, args=(store,), daemon=True, name="scheduler").start()
    yield
    store.save(force=True)


app = Starlette(routes=routes, lifespan=lifespan)
