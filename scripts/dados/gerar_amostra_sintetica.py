"""Gera uma amostra SINTÉTICA no mesmo formato dos CSVs do dataset Olist.

Serve para validar o pipeline sem o dataset real (que exige login no Kaggle). Os nomes de
arquivo, as colunas e o formato dos valores seguem o original; as proporções de status,
itens por pedido e pagamentos imitam as do Olist. NÃO usar nos experimentos do TCC.

A pasta recebe um arquivo _SINTETICO, que o seed detecta e registra em seed_info.synthetic.

Uso:
  python -m scripts.dados.gerar_amostra_sintetica --pedidos 20000 --saida data/olist-sintetico
  OLIST_DATA_DIR=./data/olist-sintetico docker compose up backend-domain-events
"""

from __future__ import annotations

import argparse
import csv
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

ESTADOS = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "DF", "GO", "ES", "PE", "CE"]
CIDADES = {"SP": "sao paulo", "RJ": "rio de janeiro", "MG": "belo horizonte", "RS": "porto alegre",
           "PR": "curitiba", "SC": "florianopolis", "BA": "salvador", "DF": "brasilia", "GO": "goiania",
           "ES": "vitoria", "PE": "recife", "CE": "fortaleza"}
CATEGORIAS = ["cama_mesa_banho", "beleza_saude", "esporte_lazer", "moveis_decoracao", "informatica_acessorios",
              "utilidades_domesticas", "relogios_presentes", "telefonia", "automotivo", "brinquedos"]
# Proporções aproximadas do dataset original.
STATUS = [("delivered", 0.970), ("shipped", 0.011), ("canceled", 0.006), ("unavailable", 0.006),
          ("invoiced", 0.003), ("processing", 0.003), ("approved", 0.001)]
TIPOS_PAGAMENTO = [("credit_card", 0.74), ("boleto", 0.19), ("voucher", 0.05), ("debit_card", 0.02)]


def hexid(rnd: random.Random) -> str:
    return uuid.UUID(int=rnd.getrandbits(128)).hex


def escolher(rnd: random.Random, pares):
    x, acc = rnd.random(), 0.0
    for v, p in pares:
        acc += p
        if x <= acc:
            return v
    return pares[-1][0]


def fmt(t: datetime | None) -> str:
    return t.strftime("%Y-%m-%d %H:%M:%S") if t else ""


def gerar(n_pedidos: int, saida: Path, semente: int) -> None:
    rnd = random.Random(semente)
    saida.mkdir(parents=True, exist_ok=True)
    n_vendedores, n_produtos = max(10, n_pedidos // 30), max(20, n_pedidos // 3)

    vendedores = []
    for _ in range(n_vendedores):
        uf = rnd.choice(ESTADOS)
        vendedores.append([hexid(rnd), f"{rnd.randint(1000, 99999):05d}", CIDADES[uf], uf])
    produtos = []
    for _ in range(n_produtos):
        produtos.append([hexid(rnd), rnd.choice(CATEGORIAS), rnd.randint(20, 60), rnd.randint(100, 3000),
                         rnd.randint(1, 6), rnd.randint(100, 20000), rnd.randint(10, 100),
                         rnd.randint(2, 100), rnd.randint(10, 100)])
    precos = {p[0]: round(rnd.lognormvariate(4.3, 0.9), 2) for p in produtos}

    clientes, pedidos, itens, pagamentos = [], [], [], []
    inicio = datetime(2016, 9, 4)
    periodo = (datetime(2018, 10, 17) - inicio).total_seconds()
    for _ in range(n_pedidos):
        uf = rnd.choice(ESTADOS)
        cliente = hexid(rnd)
        clientes.append([cliente, hexid(rnd), f"{rnd.randint(1000, 99999):05d}", CIDADES[uf], uf])

        oid = hexid(rnd)
        status = escolher(rnd, STATUS)
        compra = inicio + timedelta(seconds=rnd.uniform(0, periodo))
        aprovado = compra + timedelta(minutes=rnd.randint(5, 2 * 24 * 60)) if status != "canceled" or rnd.random() < 0.5 else None
        transportadora = aprovado + timedelta(hours=rnd.randint(12, 120)) \
            if aprovado and status in ("delivered", "shipped") else None
        entregue = transportadora + timedelta(hours=rnd.randint(24, 500)) \
            if transportadora and status == "delivered" else None
        estimada = compra + timedelta(days=rnd.randint(10, 40))
        pedidos.append([oid, cliente, status, fmt(compra), fmt(aprovado), fmt(transportadora), fmt(entregue),
                        fmt(estimada.replace(hour=0, minute=0, second=0))])

        n_itens = escolher(rnd, [(1, 0.90), (2, 0.075), (3, 0.015), (4, 0.01)])
        vendedor = rnd.choice(vendedores)[0]
        total = 0.0
        for i in range(1, n_itens + 1):
            prod = rnd.choice(produtos)[0]
            frete = round(rnd.uniform(7, 40), 2)
            total += precos[prod] + frete
            itens.append([oid, i, prod, vendedor, fmt(compra + timedelta(days=rnd.randint(3, 7))),
                          f"{precos[prod]:.2f}", f"{frete:.2f}"])

        n_pag = escolher(rnd, [(1, 0.95), (2, 0.04), (3, 0.01)])
        restante = round(total, 2)
        for s in range(1, n_pag + 1):
            valor = restante if s == n_pag else round(restante * rnd.uniform(0.2, 0.8), 2)
            restante = round(restante - valor, 2)
            tipo = escolher(rnd, TIPOS_PAGAMENTO) if s == 1 else "voucher"
            parcelas = rnd.randint(1, 10) if tipo == "credit_card" else 1
            pagamentos.append([oid, s, tipo, parcelas, f"{valor:.2f}"])

    def escrever(nome, cab, linhas):
        with open(saida / nome, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(cab)
            w.writerows(linhas)

    escrever("olist_customers_dataset.csv",
             ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
             clientes)
    escrever("olist_sellers_dataset.csv", ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
             vendedores)
    escrever("olist_products_dataset.csv",
             ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght",
              "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm",
              "product_width_cm"], produtos)
    escrever("olist_orders_dataset.csv",
             ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
              "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"],
             pedidos)
    escrever("olist_order_items_dataset.csv",
             ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price",
              "freight_value"], itens)
    escrever("olist_order_payments_dataset.csv",
             ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
             pagamentos)
    (saida / "_SINTETICO").write_text("Amostra sintética gerada por scripts/dados/gerar_amostra_sintetica.py. "
                                      "Não é o dataset Olist.\n")
    print(f"{n_pedidos} pedidos, {len(itens)} itens e {len(pagamentos)} pagamentos sintéticos em {saida}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--pedidos", type=int, default=20000)
    p.add_argument("--saida", default="data/olist-sintetico")
    p.add_argument("--semente", type=int, default=42)
    a = p.parse_args()
    gerar(a.pedidos, Path(a.saida), a.semente)
