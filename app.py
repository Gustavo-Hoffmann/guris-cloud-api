import os
import base64
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# Lidos de variáveis de ambiente no Render
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")      # ex: https://xxx.supabase.co
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")      # service_role
SUPABASE_BUCKET = os.environ.get("SUPABASE_BUCKET", "Dados")       # seu bucket


# =========================
# HELPERS SUPABASE - UPLOAD
# =========================

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


# ==========================
# HELPERS SUPABASE - DOWNLOAD
# ==========================

def _download_from_supabase(path: str):
    """
    Baixa um arquivo bruto do Supabase Storage.
    Retorna (status_code, bytes OU dict_erro).
    """
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
    """
    Lista objetos no bucket com um prefixo.
    Usa a rota oficial de listagem do Supabase:
    POST /storage/v1/object/list/{bucket}
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return 500, {"error": "SUPABASE_URL ou SUPABASE_SERVICE_KEY não configurados"}

    url = f"{SUPABASE_URL}/storage/v1/object/list/{SUPABASE_BUCKET}"

    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "apikey": SUPABASE_SERVICE_KEY,
        "Content-Type": "application/json",
    }

    body = {
        "prefix": prefix,           # ex: "participantes/"
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
        # data deve ser uma lista de objetos com campo "name"
        return 200, data
    else:
        return resp.status_code, {
            "error": "supabase_list_error",
            "status_code": resp.status_code,
            "body": data,
        }


# =========================
# HEALTHCHECK
# =========================

@app.get("/health")
def health():
    return {"status": "ok"}


# =========================
# PARSE DE UPLOAD GENÉRICO
# =========================

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


# =========================
# ENDPOINTS DE UPLOAD
# =========================

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


# =========================
# NOVO ENDPOINT: LISTAR PARTICIPANTES
# =========================

def _parse_participante_txt(texto: str):
    """
    Lê um dados.txt no formato gerado pelo app iOS:

        Nome: Fulano
        CPF: 12345678901
        Email: x@y.com
        Telefone: 519...

    Retorna dict com name, cpf, email, phone ou None se não tiver CPF válido.
    """
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
            # pega só dígitos
            digits = "".join(ch for ch in raw if ch.isdigit())
            cpf = digits if len(digits) >= 11 else None
        elif lower.startswith("email:"):
            v = linha.split(":", 1)[1].strip()
            email = None if v == "—" else v
        elif lower.startswith("telefone:"):
            v = linha.split(":", 1)[1].strip()
            telefone = None if v == "—" else v

    if not cpf:
        return None

    # Normaliza CPF para 11 dígitos (se tiver sobrado)
    if len(cpf) > 11:
        cpf = cpf[:11]

    return {
        "name": nome or "",
        "cpf": cpf,
        "email": email,
        "phone": telefone,
    }


@app.get("/list-participantes")
def list_participantes():
    """
    Lista todos os participantes salvos no Storage, lendo:
      bucket: SUPABASE_BUCKET (ex: Dados)
      prefix: "participantes/"

    Para cada .../dados.txt encontrado, baixa o arquivo, parseia e retorna JSON:

    [
      {"name": "...", "cpf": "12345678901", "email": "...", "phone": "..."},
      ...
    ]
    """
    prefix = "participantes/"

    status, data = _list_supabase_objects(prefix)
    if status != 200:
        return jsonify(data), 500

    if not isinstance(data, list):
        # algo inesperado na resposta
        return jsonify({"error": "formato inesperado do Supabase", "body": data}), 500

    participantes = []

    for obj in data:
        name = obj.get("name")
        if not name:
            continue

        # O Supabase retorna "name" relativo ao prefixo
        # Se prefix="participantes/", name pode ser "slug/dados.txt"
        full_path = f"{prefix}{name}"

        # Só queremos arquivos dados.txt
        if not full_path.endswith("/dados.txt"):
            continue

        dl_status, contents = _download_from_supabase(full_path)
        if dl_status != 200 or not isinstance(contents, (bytes, bytearray)):
            # só loga internamente, mas não quebra a listagem
            continue

        try:
            texto = contents.decode("utf-8", errors="ignore")
        except Exception:
            continue

        parsed = _parse_participante_txt(texto)
        if parsed is not None:
            participantes.append(parsed)

    # Pode ordenar por nome se quiser
    participantes.sort(key=lambda p: (p.get("name") or "").lower())

    return jsonify(participantes), 200


# =========================
# MAIN (LOCAL)
# =========================

if __name__ == "__main__":
    # Para rodar local se quiser testar antes do Render:
    # export SUPABASE_URL=...
    # export SUPABASE_SERVICE_KEY=...
    app.run(host="0.0.0.0", port=5000, debug=True)
