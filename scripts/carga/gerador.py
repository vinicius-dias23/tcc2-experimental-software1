"""Gerador de carga em malha aberta sobre a API do order-service.

A taxa de operações é mantida constante independentemente da latência da API: se o
serviço demorar a responder (como o Software A, que publica dentro da requisição), as
requisições se acumulam em voo em vez de reduzir a carga oferecida. Isso mantém a
"carga de escrita constante" exigida pelos cenários.

Cada operação recebe um X-Correlation-ID novo e é registrada no ledger (CSV), que é o
lado cliente do oráculo: tudo o que recebeu 2xx foi commitado na origem.
"""

from __future__ import annotations

import asyncio
import csv
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

import aiohttp

from scripts.comum.olist import Pedido

CAMPOS_LEDGER = ["t_envio", "t_resposta", "operacao", "order_id", "status_alvo", "correlation_id",
                 "http_status", "latencia_ms", "versao", "n_itens", "n_pagamentos", "erro"]


@dataclass
class Janela:
    """Contadores acumulados desde a última leitura (snapshot)."""
    enviadas: int = 0
    sucesso: int = 0
    erro: int = 0
    nao_enviadas: int = 0
    latencias_ms: list[float] = field(default_factory=list)


class GeradorCarga:
    def __init__(self, api_url: str, pedidos: Iterator[Pedido], taxa: float, ledger: Path,
                 max_em_voo: int = 2000, timeout_s: float = 60.0, atraso_transicao_s: float = 1.0,
                 fracao_transicoes: float = 0.5):
        """
        taxa: operações por segundo (criações + transições de status).
        atraso_transicao_s: intervalo mínimo entre operações sucessivas do mesmo pedido.
        fracao_transicoes: proporção máxima da taxa ocupada por transições pendentes;
            o restante é sempre de criações, para manter pedidos novos entrando.
        """
        self.api_url = api_url.rstrip("/")
        self.pedidos = pedidos
        self.taxa = taxa
        self.max_em_voo = max_em_voo
        self.timeout = aiohttp.ClientTimeout(total=timeout_s)
        self.atraso = atraso_transicao_s
        self.fracao_transicoes = fracao_transicoes
        self._prontas: deque[tuple[Pedido, int]] = deque()
        self._em_voo = 0
        self._tarefas: set[asyncio.Task] = set()
        self._esgotado = False
        self._retida: tuple[Pedido, int] | None = None

        self.commitadas: set[str] = set()  # correlation_ids que receberam 2xx
        self.total = Janela()
        self._janela = Janela()
        self._ledger_f = open(ledger, "w", newline="")
        self._ledger = csv.DictWriter(self._ledger_f, fieldnames=CAMPOS_LEDGER)
        self._ledger.writeheader()

    # ------------------------------------------------------------------ leitura
    def snapshot(self) -> Janela:
        """Devolve os contadores do intervalo desde a última chamada e zera o intervalo."""
        j, self._janela = self._janela, Janela()
        return j

    @property
    def em_voo(self) -> int:
        return self._em_voo

    # ------------------------------------------------------------------ execução
    async def executar(self, parar: asyncio.Event, max_operacoes: int | None = None) -> None:
        """Emite operações na taxa configurada até `parar` ser acionado (ou esgotar)."""
        conector = aiohttp.TCPConnector(limit=self.max_em_voo, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=conector, timeout=self.timeout) as sessao:
            intervalo = 1.0 / self.taxa if self.taxa > 0 else 0.0
            proximo = time.perf_counter()
            emitidas = 0
            while not parar.is_set() and (max_operacoes is None or emitidas < max_operacoes):
                op = self._proxima_operacao()
                if op is None:
                    if self._esgotado and not self._prontas and self._em_voo == 0:
                        break
                    await asyncio.sleep(0.01)
                    continue
                if self._em_voo >= self.max_em_voo:
                    if not intervalo:
                        # Sem taxa definida (rajada): espera uma vaga em vez de descartar.
                        self._retida = op
                        await asyncio.sleep(0.002)
                        continue
                    # Com taxa definida: a operação não é enviada e isso fica registrado,
                    # pois significa que a carga oferecida não foi sustentada.
                    self.total.nao_enviadas += 1
                    self._janela.nao_enviadas += 1
                    self._devolver(op)
                else:
                    t = asyncio.create_task(self._enviar(sessao, *op))
                    self._tarefas.add(t)
                    t.add_done_callback(self._tarefas.discard)
                    emitidas += 1
                if intervalo:
                    proximo += intervalo
                    espera = proximo - time.perf_counter()
                    if espera > 0:
                        await asyncio.sleep(espera)
                    elif espera < -1.0:
                        proximo = time.perf_counter()  # não tenta compensar atrasos longos
                elif emitidas % 200 == 0:
                    await asyncio.sleep(0)
            # Espera as requisições em voo terminarem (respeitando o timeout do cliente).
            if self._tarefas:
                await asyncio.gather(*list(self._tarefas), return_exceptions=True)
        self._ledger_f.flush()

    def fechar(self) -> None:
        self._ledger_f.close()

    # ------------------------------------------------------------------ interno
    def _proxima_operacao(self) -> tuple[Pedido, int] | None:
        if self._retida is not None:
            op, self._retida = self._retida, None
            return op
        # Transições prontas têm prioridade até a fração configurada; criações preenchem o resto.
        usar_transicao = self._prontas and (
            self._esgotado or (self.total.enviadas % 100) < self.fracao_transicoes * 100)
        if usar_transicao:
            return self._prontas.popleft()
        if not self._esgotado:
            try:
                return (next(self.pedidos), -1)
            except StopIteration:
                self._esgotado = True
        return self._prontas.popleft() if self._prontas else None

    def _devolver(self, op: tuple[Pedido, int]) -> None:
        if op[1] >= 0:
            self._prontas.appendleft(op)

    def _agendar(self, pedido: Pedido, indice: int) -> None:
        if indice < len(pedido.transicoes):
            asyncio.get_running_loop().call_later(self.atraso, self._prontas.append, (pedido, indice))

    async def _enviar(self, sessao: aiohttp.ClientSession, pedido: Pedido, indice: int) -> None:
        corr = str(uuid.uuid4())
        cab = {"X-Correlation-ID": corr}
        criacao = indice < 0
        status_alvo = "created" if criacao else pedido.transicoes[indice][0]
        linha = {"operacao": "create" if criacao else "status", "order_id": pedido.order_id,
                 "status_alvo": status_alvo, "correlation_id": corr,
                 "n_itens": len(pedido.itens) if criacao else 0,
                 "n_pagamentos": len(pedido.pagamentos) if criacao else 0,
                 "http_status": 0, "versao": "", "erro": ""}
        self._em_voo += 1
        self.total.enviadas += 1
        self._janela.enviadas += 1
        t0 = time.time()
        p0 = time.perf_counter()
        try:
            if criacao:
                req = sessao.post(f"{self.api_url}/orders", json=pedido.corpo_criacao(), headers=cab)
            else:
                st, quando = pedido.transicoes[indice]
                req = sessao.patch(f"{self.api_url}/orders/{pedido.order_id}/status",
                                   json={"order_status": st, "at": quando}, headers=cab)
            async with req as resp:
                linha["http_status"] = resp.status
                corpo = await resp.json(content_type=None)
                if 200 <= resp.status < 300:
                    linha["versao"] = corpo.get("version", "")
                else:
                    linha["erro"] = str(corpo.get("error", ""))[:200]
        except Exception as e:  # timeout, conexão recusada etc.
            linha["erro"] = f"{type(e).__name__}: {e}"[:200]
        finally:
            self._em_voo -= 1
        lat = (time.perf_counter() - p0) * 1000
        linha.update(t_envio=f"{t0:.6f}", t_resposta=f"{time.time():.6f}", latencia_ms=f"{lat:.2f}")
        self._ledger.writerow(linha)

        ok = 200 <= linha["http_status"] < 300
        self._janela.latencias_ms.append(lat)
        for j in (self.total, self._janela):
            if ok:
                j.sucesso += 1
            else:
                j.erro += 1
        if ok:
            self.commitadas.add(corr)
        # A próxima transição do pedido só é liberada depois da resposta desta,
        # para que as operações de um mesmo pedido não concorram entre si.
        if ok or not criacao:
            self._agendar(pedido, indice + 1)
