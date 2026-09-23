"""Leitura do dataset Olist e montagem das operações de negócio reproduzidas pela API.

A regra de corte é a mesma do seed (cmd/seed/main.go): os pedidos são ordenados por
(order_purchase_timestamp, order_id) e os primeiros floor(N * fração) vão para a carga
inicial. Os demais formam o conjunto de reprodução, usado como carga transacional.
"""

from __future__ import annotations

import csv
import hashlib
import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

ARQ_PEDIDOS = "olist_orders_dataset.csv"
ARQ_ITENS = "olist_order_items_dataset.csv"
ARQ_PAGAMENTOS = "olist_order_payments_dataset.csv"

# Status finais do Olist que não correspondem a uma coluna de data.
STATUS_FINAIS_SEM_DATA = ("canceled", "unavailable", "invoiced", "processing")


def pasta_dados() -> Path:
    """Mesma pasta montada no contêiner do seed: OLIST_DATA_DIR do ambiente ou do .env."""
    raiz = Path(__file__).resolve().parents[2]
    valor = os.environ.get("OLIST_DATA_DIR")
    env = raiz / ".env"
    if not valor and env.exists():
        for linha in env.read_text().splitlines():
            chave, _, v = linha.partition("=")
            if chave.strip() == "OLIST_DATA_DIR" and v.strip():
                valor = v.strip().strip('"').strip("'")
    return (raiz / (valor or "./data/olist")).resolve()


def _iso(ts: str) -> str | None:
    """'2017-10-02 10:56:33' -> '2017-10-02T10:56:33Z' (os horários do Olist são tratados como UTC)."""
    return ts.replace(" ", "T") + "Z" if ts else None


def _num(v: str) -> str | None:
    return v if v != "" else None


def _int(v: str) -> int | None:
    return int(float(v)) if v != "" else None


@dataclass
class Pedido:
    order_id: str
    customer_id: str
    purchase_ts: str | None
    estimated_ts: str | None
    itens: list[dict] = field(default_factory=list)
    pagamentos: list[dict] = field(default_factory=list)
    # Transições posteriores à criação, na ordem do ciclo de vida: (status, data ISO ou None).
    transicoes: list[tuple[str, str | None]] = field(default_factory=list)

    def corpo_criacao(self) -> dict:
        return {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "order_status": "created",
            "order_purchase_timestamp": self.purchase_ts,
            "order_estimated_delivery_date": self.estimated_ts,
            "items": self.itens,
            "payments": self.pagamentos,
        }

    def clone(self, rodada: int) -> "Pedido":
        """Cópia com order_id novo e determinístico, para reutilizar o conjunto em cargas longas."""
        novo = hashlib.md5(f"{self.order_id}:{rodada}".encode()).hexdigest()
        return Pedido(novo, self.customer_id, self.purchase_ts, self.estimated_ts,
                      [dict(i, order_id=novo) for i in self.itens],
                      [dict(p, order_id=novo) for p in self.pagamentos],
                      list(self.transicoes))


def _ler(caminho: Path) -> list[dict]:
    with open(caminho, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def carregar_reproducao(fracao_seed: float = 0.7, pasta: Path | None = None) -> list[Pedido]:
    """Devolve os pedidos que NÃO entraram na carga inicial, em ordem cronológica."""
    pasta = pasta or pasta_dados()
    if not (pasta / ARQ_PEDIDOS).exists():
        raise SystemExit(f"{ARQ_PEDIDOS} não encontrado em {pasta}. Veja o README (seção Dados).")
    pedidos = _ler(pasta / ARQ_PEDIDOS)
    pedidos.sort(key=lambda r: (r["order_purchase_timestamp"], r["order_id"]))
    corte = math.floor(len(pedidos) * fracao_seed)
    resto = pedidos[corte:]
    ids = {r["order_id"] for r in resto}

    itens: dict[str, list[dict]] = {}
    for r in _ler(pasta / ARQ_ITENS):
        if r["order_id"] in ids:
            itens.setdefault(r["order_id"], []).append({
                "order_id": r["order_id"],
                "order_item_id": int(r["order_item_id"]),
                "product_id": r["product_id"] or None,
                "seller_id": r["seller_id"] or None,
                "shipping_limit_date": _iso(r["shipping_limit_date"]),
                "price": _num(r["price"]),
                "freight_value": _num(r["freight_value"]),
            })
    pagamentos: dict[str, list[dict]] = {}
    for r in _ler(pasta / ARQ_PAGAMENTOS):
        if r["order_id"] in ids:
            pagamentos.setdefault(r["order_id"], []).append({
                "order_id": r["order_id"],
                "payment_sequential": int(r["payment_sequential"]),
                "payment_type": r["payment_type"] or None,
                "payment_installments": _int(r["payment_installments"]),
                "payment_value": _num(r["payment_value"]),
            })

    saida = []
    for r in resto:
        trans: list[tuple[str, str | None]] = []
        if r["order_approved_at"]:
            trans.append(("approved", _iso(r["order_approved_at"])))
        if r["order_delivered_carrier_date"]:
            trans.append(("shipped", _iso(r["order_delivered_carrier_date"])))
        if r["order_delivered_customer_date"]:
            trans.append(("delivered", _iso(r["order_delivered_customer_date"])))
        if r["order_status"] in STATUS_FINAIS_SEM_DATA:
            trans.append((r["order_status"], None))
        saida.append(Pedido(r["order_id"], r["customer_id"], _iso(r["order_purchase_timestamp"]),
                            _iso(r["order_estimated_delivery_date"]),
                            itens.get(r["order_id"], []), pagamentos.get(r["order_id"], []), trans))
    return saida


def fluxo_pedidos(pedidos: list[Pedido], repetir: bool = True, inicio_rodada: int = 0) -> Iterator[Pedido]:
    """Itera o conjunto de reprodução; com repetir=True, recomeça com clones de order_id novo."""
    rodada = inicio_rodada
    while True:
        for p in pedidos:
            yield p if rodada == 0 else p.clone(rodada)
        if not repetir:
            return
        rodada += 1
