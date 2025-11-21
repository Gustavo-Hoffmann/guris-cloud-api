import os
import base64
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# Lidos de variáveis de ambiente no Render
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")      # ex: https://zbwrjgmsclicjwanxwgt.supabase.co
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")      # service_role
SUPABASE_BUCKET = os.environ.get("SUPABASE_BUCKET", "Dados")       # seu bucket


def _upload_to_supabase(path: str, file_bytes: bytes, content_type: str):
    """Envia bytes para o Supabase Storage usando a service_role."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return 500, {"error": "SUPABASE_URL ou SUPABASE_SERVICE_KEY não configurados"}

    safe_path = path.lstrip("/")  # garante que não começa com /
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{safe_path}"

    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "apikey": SUPABASE_SERVICE_KEY,
        "Content-Type": content_type,
        "x-upsert": "true",  # sobrescreve se já existir
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


@app.get("/health")
def health():
    return {"status": "ok"}


def _parse_upload_request(default_content_type: str):
    """
    Espera JSON:
    {
      "path": "coletas/.../dados.csv",
      "content_base64": "...",
      "content_type": "text/csv" (opcional, cai no default se faltar)
    }
    """
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


if __name__ == "__main__":
    # Para rodar local se quiser testar antes do Render:
    # export SUPABASE_URL=...
    # export SUPABASE_SERVICE_KEY=...
    app.run(host="0.0.0.0", port=5000, debug=True)
