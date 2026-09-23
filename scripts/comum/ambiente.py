"""Controle do ambiente Docker Compose e leitura de sinais dos serviços.

Também serve para alternar o backend no ar, derrubando o outro e zerando as bases:
  python -m scripts.comum.ambiente domain-events
  python -m scripts.comum.ambiente cdc
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
from pathlib import Path

import aiohttp

RAIZ = Path(__file__).resolve().parents[2]

API_URL = os.environ.get("API_URL", "http://localhost:8080")
CONSUMIDOR_URL = os.environ.get("CONSUMIDOR_URL", "http://localhost:8081")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
ORIGEM_DSN = os.environ.get("ORIGEM_DSN", "postgresql://olist:olist@localhost:5432/olist")
DESTINO_DSN = os.environ.get("DESTINO_DSN", "postgresql://olist:olist@localhost:5433/olist")
CONECTOR = os.environ.get("CONNECTOR_NAME", "olist-origem")
SLOT = os.environ.get("SLOT_NAME", "olist_debezium")

MODOS = {
    "domain-events": {"alvo": "backend-domain-events", "profile": "domain-events",
                      "order": "order-service-domain-events", "consumidor": "shipping-service-domain-events"},
    "cdc": {"alvo": "backend-change-data-capture", "profile": "cdc",
            "order": "order-service-cdc", "consumidor": "shipping-service-cdc"},
}
BROKERS = ["kafka-1", "kafka-2", "kafka-3"]


def compose(*args: str, check: bool = True, capturar: bool = False, timeout: float | None = None
            ) -> subprocess.CompletedProcess:
    """Executa `docker compose` na raiz do repositório com os dois profiles habilitados."""
    cmd = ["docker", "compose", "--profile", "domain-events", "--profile", "cdc", *args]
    r = subprocess.run(cmd, cwd=RAIZ, text=True, timeout=timeout, capture_output=True)
    if not capturar:
        print(r.stdout + r.stderr, end="", flush=True)
    if check and r.returncode != 0:
        raise SystemExit(f"falhou: {' '.join(cmd)}\n{r.stdout}{r.stderr}")
    return r


async def compose_async(*args: str) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        "docker", "compose", "--profile", "domain-events", "--profile", "cdc", *args,
        cwd=RAIZ, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    out, _ = await proc.communicate()
    return proc.returncode, out.decode(errors="replace")


def reiniciar_ambiente(modo: str, build: bool = False) -> None:
    """Derruba tudo (inclusive volumes) e sobe o backend do modo pedido, esperando ficar saudável.

    Reproduz a reinicialização integral entre execuções descrita na Seção 4.3: bancos
    recarregados do seed, tópicos recriados e slot de replicação recriado.
    """
    alvo = MODOS[modo]["alvo"]
    print("[ambiente] derrubando o ambiente anterior")
    compose("down", "-v", "--remove-orphans")
    print(f"[ambiente] subindo {alvo}")
    args = ["up", "-d", "--wait", "--wait-timeout", "600"]
    if build:
        args.append("--build")
    compose(*args, alvo)
    esperar_http(f"{API_URL}/healthz", 120)
    esperar_http(f"{CONSUMIDOR_URL}/healthz", 120)
    if modo == "cdc":
        esperar_conector_running(180)
    print(f"[ambiente] {alvo} pronto")


def esperar_http(url: str, timeout_s: float) -> None:
    import urllib.request
    limite = time.time() + timeout_s
    while True:
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                if r.status == 200:
                    return
        except Exception:
            pass
        if time.time() > limite:
            raise SystemExit(f"{url} não respondeu em {timeout_s:.0f}s")
        time.sleep(1)


def esperar_conector_running(timeout_s: float) -> None:
    import urllib.request
    limite = time.time() + timeout_s
    while time.time() < limite:
        try:
            with urllib.request.urlopen(f"{CONNECT_URL}/connectors/{CONECTOR}/status", timeout=3) as r:
                st = json.load(r)
                if st["connector"]["state"] == "RUNNING" and st["tasks"] and \
                        all(t["state"] == "RUNNING" for t in st["tasks"]):
                    return
        except Exception:
            pass
        time.sleep(2)
    raise SystemExit("o conector Debezium não chegou a RUNNING")


async def ler_metricas(sessao: aiohttp.ClientSession, url: str) -> dict[str, float]:
    """Lê o /metrics (texto Prometheus) de um serviço. Devolve {} se não responder."""
    try:
        async with sessao.get(f"{url}/metrics", timeout=aiohttp.ClientTimeout(total=3)) as r:
            texto = await r.text()
    except Exception:
        return {}
    out = {}
    for linha in texto.splitlines():
        if linha and not linha.startswith("#"):
            nome, _, valor = linha.partition(" ")
            try:
                out[nome] = float(valor)
            except ValueError:
                pass
    return out


async def estado_conector(sessao: aiohttp.ClientSession) -> str:
    """Estado da tarefa Debezium (RUNNING, FAILED, PAUSED...) ou INALCANCAVEL."""
    try:
        async with sessao.get(f"{CONNECT_URL}/connectors/{CONECTOR}/status",
                              timeout=aiohttp.ClientTimeout(total=3)) as r:
            st = await r.json()
        tarefas = st.get("tasks") or []
        return tarefas[0]["state"] if tarefas else st["connector"]["state"]
    except Exception:
        return "INALCANCAVEL"


async def reiniciar_tarefas_falhas(sessao: aiohttp.ClientSession) -> bool:
    """Pede ao Connect que reinicie conector e tarefas com falha (intervenção operacional)."""
    try:
        async with sessao.post(f"{CONNECT_URL}/connectors/{CONECTOR}/restart?includeTasks=true&onlyFailed=true",
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            return r.status < 300
    except Exception:
        return False


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2 or sys.argv[1] not in MODOS:
        raise SystemExit(f"uso: python -m scripts.comum.ambiente {{{'|'.join(MODOS)}}}")
    reiniciar_ambiente(sys.argv[1])
