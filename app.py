import os
import base64
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
SUPABASE_BUCKET = os.environ.get("SUPABASE_BUCKET", "Dados")


def _upload_to_supabase(path: str, file_bytes: bytes, content_type: str):
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return 500, {"error": "SUPABASE_URL ou SUPABASE_SERVICE_KEY não configurados"}

    safe_path = path.lstrip("/")
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{safe_path}"

    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "apikey": SUPABASE_SERVICE_KEY,
        "Content-Type": content_type,
        "x-upsert": "true",
    }

    resp = requests.put(url, data=file_bytes, headers=headers)

    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}

    if 200 <= resp.status_code < 300:
        return 200, {"status": "ok", "supabase_key": body.get("Key")}
    else:
        return resp.status_code, {
            "error": "supabase_error",
            "status_code": resp.status_code,
            "body": body,
        }


def _download_from_supabase(path: str):
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return 500, {"error": "SUPABASE_URL ou SUPABASE_SERVICE_KEY não configurados"}

    safe_path = path.lstrip("/")
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{safe_path}"

    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "apikey": SUPABASE_SERVICE_KEY,
    }

    resp = requests.get(url, headers=headers)
    if 200 <= resp.status_code < 300:
        return resp.status_code, resp.content
    else:
        try:
            body = resp.json()
        except Exception:
            body = {"raw": resp.text}
        return resp.status_code, {
            "error": "supabase_download_error",
            "status_code": resp.status_code,
            "body": body,
        }


def _list_supabase_objects(prefix: str, limit: int = 1000):
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return 500, {"error": "SUPABASE_URL ou SUPABASE_SERVICE_KEY não configurados"}

    url = f"{SUPABASE_URL}/storage/v1/object/list/{SUPABASE_BUCKET}"

    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "apikey": SUPABASE_SERVICE_KEY,
        "Content-Type": "application/json",
    }

    body = {
        "prefix": prefix,
        "limit": limit,
        "offset": 0,
        "sortBy": {"column": "name", "order": "asc"},
    }

    resp = requests.post(url, json=body, headers=headers)
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}

    if 200 <= resp.status_code < 300:
        return 200, data
    else:
        return resp.status_code, {
            "error": "supabase_list_error",
            "status_code": resp.status_code,
            "body": data,
        }


@app.get("/health")
def health():
    return {"status": "ok"}


def _parse_upload_request(default_content_type: str):
    data = request.get_json(silent=True)
    if not data:
        return None, None, None, (400, {"error": "JSON inválido ou ausente"})

    path = data.get("path")
    content_b64 = data.get("content_base64")
    content_type = data.get("content_type") or default_content_type

    if not path or not content_b64:
        return None, None, None, (
            400,
            {"error": "Campos 'path' e 'content_base64' são obrigatórios"},
        )

    try:
        file_bytes = base64.b64decode(content_b64)
    except Exception:
        return None, None, None, (400, {"error": "content_base64 inválido"})

    return path, file_bytes, content_type, None


@app.post("/upload-coleta")
def upload_coleta():
    path, file_bytes, content_type, error = _parse_upload_request("text/csv")
    if error:
        status, body = error
        return jsonify(body), status

    status, body = _upload_to_supabase(path, file_bytes, content_type)
    return jsonify(body), (200 if status == 200 else 500)


@app.post("/upload-pesquisador")
def upload_pesquisador():
    path, file_bytes, content_type, error = _parse_upload_request("text/plain")
    if error:
        status, body = error
        return jsonify(body), status

    status, body = _upload_to_supabase(path, file_bytes, content_type)
    return jsonify(body), (200 if status == 200 else 500)


@app.post("/upload-participante")
def upload_participante():
    path, file_bytes, content_type, error = _parse_upload_request("text/plain")
    if error:
        status, body = error
        return jsonify(body), status

    status, body = _upload_to_supabase(path, file_bytes, content_type)
    return jsonify(body), (200 if status == 200 else 500)


# PATCH APLICADO AQUI 👇
def _parse_participante_txt(texto: str):
    nome = None
    cpf = None
    email = None
    telefone = None

    for linha in texto.splitlines():
        linha = linha.strip()
        if not linha:
            continue

        lower = linha.lower()
        if lower.startswith("nome:"):
            nome = linha.split(":", 1)[1].strip()
        elif lower.startswith("cpf:"):
            raw = linha.split(":", 1)[1].strip()
            digits = "".join(ch for ch in raw if ch.isdigit())
            cpf = digits[:11] if digits else "00000000000"
        elif lower.startswith("email:"):
            v = linha.split(":", 1)[1].strip()
            email = None if v == "—" else v
        elif lower.startswith("telefone:"):
            v = linha.split(":", 1)[1].strip()
            telefone = None if v == "—" else v

    return {
        "name": nome or "",
        "cpf": cpf or "00000000000",
        "email": email,
        "phone": telefone,
    }


@app.get("/list-participantes")
def list_participantes():
    prefix = "participantes/"
    status, data = _list_supabase_objects(prefix)
    if status != 200:
        return jsonify(data), 500

    if not isinstance(data, list):
        return jsonify({"error": "formato inesperado do Supabase", "body": data}), 500

    participantes = []

    for obj in data:
        name = obj.get("name")
        if not name:
            continue
        full_path = f"{prefix}{name}"
        if not full_path.endswith("/dados.txt"):
            continue

        dl_status, contents = _download_from_supabase(full_path)
        if dl_status != 200 or not isinstance(contents, (bytes, bytearray)):
            continue

        try:
            texto = contents.decode("utf-8", errors="ignore")
        except Exception:
            continue

        parsed = _parse_participante_txt(texto)
        if parsed is not None:
            participantes.append(parsed)

    participantes.sort(key=lambda p: (p.get("name") or "").lower())
    return jsonify(participantes), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)