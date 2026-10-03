"""All control-plane state (connections, pipelines, runs, checks, logs) lives in one JSON document.

Locally it is a file. On Cloud Run set STATE_URI=gs://bucket/state.json so it survives restarts.
The data itself never lives here: it stays in the warehouse.
"""
import json
import os
import threading
import time

import requests

from .config import settings
from .util import now_ms

EMPTY = {
    "seq": 1, "connections": [], "pipelines": [], "runs": [], "logs": [], "alerts": [], "rules": [],
    "quarantine": [], "mappings": {}, "drift": [], "audit": [], "approvals": [], "tstats": {},
    "toolApproval": {}, "roles": [], "version": 1,
}
LIMITS = {"runs": 400, "logs": 1500, "audit": 400, "alerts": 200, "quarantine": 200, "approvals": 100}


class Store:
    def __init__(self, uri=None):
        self.uri = uri or settings.state_uri
        self.lock = threading.RLock()
        self.data = json.loads(json.dumps(EMPTY))
        self._dirty = False
        self._last_save = 0
        self.load()

    # ----- persistence -----
    def _gcs(self):
        bucket, _, obj = self.uri[5:].partition("/")
        return bucket, obj

    def load(self):
        raw = None
        if self.uri.startswith("gs://"):
            from .cloudauth import gcp_headers
            bucket, obj = self._gcs()
            r = requests.get(f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{requests.utils.quote(obj, safe='')}",
                             params={"alt": "media"}, headers=gcp_headers(), timeout=30)
            if r.status_code == 200:
                raw = r.text
            elif r.status_code != 404:
                raise RuntimeError(f"Could not read state from {self.uri}: {r.status_code} {r.text[:200]}")
        elif os.path.exists(self.uri):
            with open(self.uri, encoding="utf-8") as f:
                raw = f.read()
        if raw:
            loaded = json.loads(raw)
            for k, v in EMPTY.items():
                loaded.setdefault(k, json.loads(json.dumps(v)))
            self.data = loaded
            # a run that was in flight when the service stopped can never finish
            for r in self.data["runs"]:
                if r.get("status") == "running":
                    r["status"] = "failed"
                    r["end"] = now_ms()
                    r["failMsg"] = "the service restarted while this run was in progress"
                    for t in r.get("tasks", {}).values():
                        if t["status"] in ("running", "retry"):
                            t["status"] = "failed"
                        elif t["status"] == "queued":
                            t["status"] = "upstream_failed"

    def save(self, force=False):
        with self.lock:
            self._dirty = True
            if not force and time.time() - self._last_save < 1.0:
                return
            self._flush()

    def flush_if_dirty(self):
        with self.lock:
            if self._dirty:
                self._flush()

    def _flush(self):
        for key, n in LIMITS.items():
            if len(self.data[key]) > n:
                self.data[key] = self.data[key][:n]
        body = json.dumps(self.data, separators=(",", ":"))
        if self.uri.startswith("gs://"):
            from .cloudauth import gcp_headers
            bucket, obj = self._gcs()
            r = requests.post(f"https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o",
                              params={"uploadType": "media", "name": obj},
                              headers={**gcp_headers(), "Content-Type": "application/json"}, data=body.encode(), timeout=60)
            if not r.ok:
                raise RuntimeError(f"Could not save state to {self.uri}: {r.status_code} {r.text[:200]}")
        else:
            os.makedirs(os.path.dirname(os.path.abspath(self.uri)), exist_ok=True)
            tmp = self.uri + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(body)
            os.replace(tmp, self.uri)
        self._dirty = False
        self._last_save = time.time()

    # ----- helpers -----
    def next_id(self, prefix):
        with self.lock:
            self.data["seq"] += 1
            return f"{prefix}{self.data['seq']}"

    def find(self, key, id_):
        return next((x for x in self.data[key] if x.get("id") == id_), None)

    def pipeline(self, pid):
        return self.find("pipelines", pid)

    def connection(self, cid):
        return self.find("connections", cid)

    def audit(self, action, target, detail="", who="you"):
        with self.lock:
            self.data["audit"].insert(0, {"ts": now_ms(), "who": who, "action": action, "target": target,
                                          "detail": detail, "env": settings.environment})
        self.save()

    def log(self, level, pid, task, msg, run_id=None):
        with self.lock:
            self.data["logs"].insert(0, {"ts": now_ms(), "level": level, "pid": pid, "task": task, "msg": msg, "runId": run_id})
        self.save()

    def alert(self, sev, title, detail, view="orchestration", pid=None):
        a = {"id": self.next_id("al"), "ts": now_ms(), "sev": sev, "title": title, "detail": detail,
             "view": view, "pid": pid, "ack": False}
        with self.lock:
            self.data["alerts"].insert(0, a)
        self.save()
        return a
