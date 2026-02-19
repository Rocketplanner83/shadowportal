import json
import os
from datetime import datetime, timezone
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, Response, jsonify, send_file, abort, g
from flask_caching import Cache

from config import settings
from utils.zfs import ZfsError, validate_restore_paths, TrueNASClient
from services.zfs_service import ZfsService

# single service instance used by routes
zfs_service = ZfsService()

# ---- CRITICAL: Gunicorn expects module-level variable named "app" ----
app = Flask(__name__)
app.secret_key = settings.FLASK_SECRET_KEY

# Log presence (not values) of TrueNAS configuration for startup debugging
try:
    app.logger.info(
        "Config status: URL=%s API_KEY=%s VERIFY_TLS=%s",
        bool(settings.TRUENAS_URL),
        bool(settings.TRUENAS_API_KEY),
        bool(settings.TRUENAS_VERIFY_TLS),
    )
except Exception:
    # Logging should never prevent startup
    pass

# Explicit export for WSGI loaders
__all__ = ["app"]

app.config["CACHE_TYPE"] = "SimpleCache"
app.config["CACHE_DEFAULT_TIMEOUT"] = int(os.getenv("CACHE_DEFAULT_TIMEOUT", 60))
cache = Cache(app)


def validate_truenas_connectivity(client: TrueNASClient):
    zfs_service.validate_connectivity(client)


def audit_log(action: str, details: dict):
    try:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "user": session.get("user", "anonymous"),
            "role": session.get("role", "none"),
            "ip": request.headers.get("X-Forwarded-For", request.remote_addr),
            "action": action,
            "details": details,
        }
        with open(settings.AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _safe_next(next_url: str | None) -> str | None:
    """Prevent open-redirects and avoid redirecting to POST-only action endpoints."""
    if not next_url:
        return None
    if not next_url.startswith("/"):
        return None
    # Do not redirect into action routes that require POST
    if next_url.startswith("/rollback/") or next_url.startswith("/clone/"):
        return None
    return next_url


def require_login(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("role"):
            # preserve path only; avoid including full URL
            return redirect(url_for("login", next=request.path))
        return fn(*args, **kwargs)

    return wrapper


def require_admin(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if session.get("role") != "admin":
            return "Forbidden", 403
        return fn(*args, **kwargs)

    return wrapper


def require_truenas(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        # Avoid importing at module import time to prevent side effects
        from config import is_configured

        if not is_configured():
            return render_template("config_error.html"), 503
        return fn(*args, **kwargs)

    return wrapper


def with_truenas_client(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        client = TrueNASClient()
        try:
            client.connect()
            g.truenas_client = client
            return fn(*args, **kwargs)
        finally:
            try:
                client.close()
            finally:
                g.pop("truenas_client", None)

    return wrapper


# ---- IMPORTANT ----
# Do NOT execute connectivity validation at import time.
# Gunicorn imports this module to locate `app`.
# Running network calls here can break worker boot.


def friendly_date(raw_ts):
    if not raw_ts:
        return "unknown"
    if raw_ts.tzinfo is None:
        raw_ts = raw_ts.replace(tzinfo=timezone.utc)
    diff = datetime.now(timezone.utc) - raw_ts
    seconds = int(diff.total_seconds())
    if seconds < 60:
        return "just now"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    return raw_ts.strftime("%b %d, %Y")


# Flask 3.x removed before_first_request.
# Connectivity validation should be triggered explicitly via a health endpoint
# or performed lazily on first real API interaction, not during import.


@app.route("/login", methods=["GET", "POST"])
def login():
    err = None
    if request.method == "POST":
        user = (request.form.get("user") or "").strip() or "user"
        pw = request.form.get("password") or ""

        import bcrypt

        next_url = _safe_next(request.args.get("next")) or url_for("index")

        if settings.ADMIN_PASSWORD_HASH:
            try:
                if bcrypt.checkpw(pw.encode(), settings.ADMIN_PASSWORD_HASH.encode()):
                    session["user"] = user
                    session["role"] = "admin"
                    audit_log("login", {"as": "admin"})
                    return redirect(next_url)
            except Exception:
                pass

        if settings.VIEWER_PASSWORD_HASH:
            try:
                if bcrypt.checkpw(pw.encode(), settings.VIEWER_PASSWORD_HASH.encode()):
                    session["user"] = user
                    session["role"] = "viewer"
                    audit_log("login", {"as": "viewer"})
                    return redirect(next_url)
            except Exception:
                pass

        err = "Invalid credentials"

    return render_template("login.html", error=err)


@app.route("/health")
@require_truenas
@with_truenas_client
def health():
    """Cheap health probe for reverse proxies and quick debugging."""
    try:
        zfs_service.validate_connectivity(g.truenas_client)
        return {"ok": True, "truenas": "ok"}
    except Exception as e:
        return {"ok": False, "truenas": "error", "error": str(e)}, 503


@app.route("/logout")
def logout():
    audit_log("logout", {})
    session.clear()
    return redirect(url_for("login"))


@app.errorhandler(ZfsError)
def handle_zfs_error(e):
    return render_template("error.html", message=str(e)), 500


@app.route("/")
@require_truenas
@require_login
@with_truenas_client
def index():
    datasets = zfs_service.list_datasets(client=g.truenas_client)
    pools = zfs_service.build_pool_tree(datasets, client=g.truenas_client)

    return render_template(
        "index.html",
        pools=pools,
        role=session.get("role"),
        user=session.get("user"),
    )


@app.route("/dataset")
@require_truenas
@require_login
@with_truenas_client
def dataset_view():
    dataset = request.args.get("name")
    if not dataset:
        abort(400)

    snapshots = zfs_service.list_snapshots(dataset, client=g.truenas_client)
    snapshots.sort(
        key=lambda x: x.get("created_at") or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    for s in snapshots:
        s["age"] = friendly_date(s.get("created_at"))

    return render_template(
        "dataset.html",
        dataset=dataset,
        snapshots=snapshots,
        role=session.get("role"),
        user=session.get("user"),
    )


@app.route("/rollback/<path:dataset>/<snapshot>", methods=["POST"])
@require_truenas
@require_login
@require_admin
@with_truenas_client
def rollback(dataset, snapshot):
    audit_log("rollback", {"dataset": dataset, "snapshot": snapshot})
    result = zfs_service.rollback_snapshot(dataset, snapshot, client=g.truenas_client)
    return {"ok": True, "result": result}


@app.route("/clone/<path:dataset>/<snapshot>", methods=["POST"])
@require_truenas
@require_login
@require_admin
@with_truenas_client
def clone(dataset, snapshot):
    target = request.form.get("target")
    if not target:
        return {"ok": False, "error": "Clone target required"}, 400
    audit_log("clone", {"dataset": dataset, "snapshot": snapshot, "target": target})
    result = zfs_service.clone_snapshot(dataset, snapshot, target, client=g.truenas_client)
    return {"ok": True, "result": result}


@app.route("/diff/<path:dataset>")
@require_truenas
@require_login
@with_truenas_client
def diff_view(dataset):
    a = request.args.get("a")
    b = request.args.get("b")
    if not a or not b:
        return "Missing a/b", 400
    d = zfs_service.snapshot_diff(dataset, a, b, client=g.truenas_client)
    audit_log("diff", {"dataset": dataset, "a": a, "b": b})
    return render_template("diff.html", diff=d, dataset=dataset, a=a, b=b)


@app.route("/audit")
@require_login
@require_admin
def audit_view():
    rows = []
    try:
        if os.path.exists(settings.AUDIT_LOG_PATH):
            with open(settings.AUDIT_LOG_PATH, "r", encoding="utf-8") as f:
                for line in f.readlines()[-400:]:
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        continue
    except Exception:
        pass
    rows.reverse()
    return render_template("audit.html", rows=rows)


@app.route("/browse")
@require_truenas
@require_login
@with_truenas_client
def browse_snapshot():
    dataset = request.args.get("dataset")
    snapshot = request.args.get("snapshot")
    subpath = request.args.get("subpath", "")

    # Minimal logging
    app.logger.info("browse_snapshot request dataset=%s snapshot=%s subpath=%s", dataset, snapshot, subpath)

    if not dataset or not snapshot:
        return render_template("error.html", message="Missing dataset or snapshot"), 400

    import posixpath
    from urllib.parse import unquote_plus
    decoded_subpath = unquote_plus(subpath or "")
    if decoded_subpath in {"", "/", "."}:
        current_path = ""
    else:
        current_path = posixpath.normpath(decoded_subpath.strip("/"))
        if current_path in {".", ".."} or current_path.startswith("../"):
            return render_template("error.html", message="Invalid snapshot subpath"), 400

    breadcrumbs = []
    if current_path:
        accum = []
        for part in current_path.split("/"):
            if not part:
                continue
            accum.append(part)
            breadcrumbs.append((part, "/".join(accum)))

    try:
        entries = zfs_service.list_snapshot_files(dataset, snapshot, current_path, client=g.truenas_client)
    except Exception as e:
        app.logger.exception("Browse snapshot failed")
        return render_template("error.html", message=str(e)), 400

    return render_template(
        "snapshot_browser.html",
        dataset=dataset,
        snapshot=snapshot,
        subpath=current_path,
        current_path=current_path,
        breadcrumbs=breadcrumbs,
        entries=entries,
        role=session.get("role"),
    )


@app.route("/download/<path:dataset>/<snapshot>/<path:filepath>")
@require_truenas
@require_login
def download_snapshot_file(dataset, snapshot, filepath):
    base = os.path.join("/data", dataset, ".zfs", "snapshot", snapshot)
    full = os.path.normpath(os.path.join(base, filepath))

    if not full.startswith(base) or not os.path.exists(full):
        abort(404)

    return send_file(full, as_attachment=True)


@app.route("/restore-file", methods=["POST"])
@require_truenas
@require_login
@require_admin
@with_truenas_client
def restore_file():
    data = request.get_json(silent=True) or {}
    dataset = data.get("dataset")
    snapshot = data.get("snapshot")
    path = data.get("path")

    if not dataset or not snapshot or not path:
        return jsonify({"ok": False, "error": "Missing dataset/snapshot/path"}), 400
    src = os.path.join("/data", dataset, ".zfs", "snapshot", snapshot, path)
    dest = os.path.join("/data", dataset, path)

    try:
        # validate local container paths
        validate_restore_paths(src, dest)
    except ZfsError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    try:
        job_id = zfs_service.restore_path(
            dataset,
            snapshot,
            path,
            dest,
            overwrite=bool(data.get("overwrite", False)),
            client=g.truenas_client,
        )
    except Exception as e:
        app.logger.exception("restore_path failed")
        return jsonify({"ok": False, "error": str(e)}), 500

    audit_log("restore_file", {"dataset": dataset, "snapshot": snapshot, "path": path, "job_id": job_id})
    return jsonify({"job_id": job_id, "status": "submitted"})


@app.route("/events/jobs/<int:job_id>")
@require_truenas
@require_login
def job_events(job_id):
    def gen():
        from uuid import uuid4

        client = TrueNASClient()
        client.connect()
        sub_id = str(uuid4())
        try:
            client.subscribe("core.get_jobs", sub_id)

            j = zfs_service.get_job(job_id, client=client)
            initial_state = (j or {}).get("state", "UNKNOWN")
            payload = json.dumps({
                "id": job_id,
                "state": initial_state,
            })
            yield f"data: {payload}\n\n"

            while True:
                msg = client.recv()
                if msg.get("collection") != "core.get_jobs":
                    continue

                fields = msg.get("fields") or {}
                if fields.get("id") != job_id:
                    continue

                payload_dict = {
                    "id": fields.get("id"),
                    "state": fields.get("state"),
                    "progress": fields.get("progress"),
                    "error": fields.get("error"),
                }

                payload_json = json.dumps(payload_dict)
                yield f"data: {payload_json}\n\n"

                if payload_dict.get("state") in {"SUCCESS", "FAILED", "ABORTED"}:
                    break
        finally:
            try:
                client.unsubscribe(sub_id)
            except Exception:
                pass
            client.close()

    return Response(gen(), mimetype="text/event-stream")


@app.route("/api/jobs/<int:job_id>")
@require_truenas
@require_login
@with_truenas_client
def api_get_job(job_id: int):
    try:
        raw = zfs_service.get_job(job_id, client=g.truenas_client)
        job = None
        if isinstance(raw, list) and raw:
            job = raw[0]
        elif isinstance(raw, dict):
            job = raw

        if not job:
            return jsonify({"ok": False, "error": "job not found"}), 404

        info = {
            "id": job.get("id"),
            "state": job.get("state"),
            "progress": job.get("progress"),
            "error": job.get("error"),
            "result": job.get("result"),
        }

        return jsonify({"ok": True, "job": info})
    except Exception as e:
        app.logger.exception("api_get_job failed")
        return jsonify({"ok": False, "error": str(e)}), 500
