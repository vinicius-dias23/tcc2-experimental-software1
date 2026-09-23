"""Cenário 1 — Indisponibilidade da fila com a aplicação aceitando escritas.

Cada execução segue as três fases da Seção 4.4:

  1. regime       : carga constante com o cluster Kafka saudável (linha de base);
  2. falha        : os brokers são interrompidos pela duração da janela, e a carga continua;
  3. recuperação  : os brokers voltam, a carga continua por um tempo e depois para; a coleta
                    segue até a convergência entre origem e destino ou até o limite.

Por padrão o ambiente é reinicializado por completo antes de cada execução (down -v e up),
e as janelas seguem o desenho do cenário: 5, 15, 30 e 60 minutos.

Exemplos (na raiz do repositório, com o venv ativo):

  # desenho completo para o Software A
  python -m scripts.cenario1.fila_indisponivel --modo domain-events

  # os dois softwares, janelas de 5 e 15 min, 3 repetições cada
  python -m scripts.cenario1.fila_indisponivel --modo ambos --janelas 5,15 --repeticoes 3

  # ensaio rápido (janela de 60 s)
  python -m scripts.cenario1.fila_indisponivel --modo cdc --janela-segundos 60 --regime 30 --recuperacao 30

Saída em resultados/cenario1/<execução>/: ledger.csv (uma linha por requisição),
serie_temporal.csv (uma amostra a cada --intervalo s), marcos.json, exportações das bases,
logs dos contêineres e resumo.json com as métricas. Um consolidado.csv acumula as execuções.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import statistics
import time
from datetime import datetime
from pathlib import Path

import aiohttp
import psycopg

from scripts.carga.gerador import GeradorCarga
from scripts.comum import ambiente as amb
from scripts.comum.analise import analisar, imprimir
from scripts.comum.coleta import Coletor
from scripts.comum.olist import carregar_reproducao, fluxo_pedidos

CAMPOS_SERIE = ["t", "instante", "fase", "api_enviadas", "api_sucesso", "api_erro", "api_nao_enviadas",
                "api_p99_ms", "em_voo", "commitadas_total", "destino_mensagens_total", "destino_correlacoes",
                "gap", "divergencia", "publicacao_ok_total", "publicacao_falhas_total", "conector_estado",
                "origem_disponivel", "origem_db_bytes", "origem_wal_bytes", "origem_pedidos_tocados",
                "slot_ativo", "slot_retido_bytes", "slot_atraso_bytes"]


class Execucao:
    def __init__(self, modo: str, janela_s: float, args: argparse.Namespace, pasta: Path):
        self.modo, self.janela_s, self.args, self.pasta = modo, janela_s, args, pasta
        self.fase = "regime"
        self.ultima: dict = {}
        self.marcos: dict = {"modo": modo, "parametros": {
            "janela_s": janela_s, "taxa_ops_s": args.taxa, "regime_s": args.regime,
            "recuperacao_carga_s": args.recuperacao, "brokers": args.brokers, "parada": args.parada,
            "intervalo_amostra_s": args.intervalo, "reiniciar_conector_falho": args.reiniciar_conector_falho}}

    async def rodar(self, pedidos) -> dict:
        a = self.args
        inicio = time.time()
        self.marcos["inicio"] = inicio
        self.coletor = Coletor(inicio)
        await self.coletor.conectar()
        self.gerador = GeradorCarga(amb.API_URL, fluxo_pedidos(pedidos, inicio_rodada=a.rodada_inicial), a.taxa, self.pasta / "ledger.csv",
                                    max_em_voo=a.max_em_voo)
        parar_carga, parar_amostra = asyncio.Event(), asyncio.Event()
        async with aiohttp.ClientSession() as sessao:
            self.sessao = sessao
            t_carga = asyncio.create_task(self.gerador.executar(parar_carga))
            t_amostra = asyncio.create_task(self._amostrador(parar_amostra))

            self._log(f"regime por {a.regime:.0f}s a {a.taxa} ops/s")
            await asyncio.sleep(a.regime)

            self.fase = "falha"
            self.marcos["falha_inicio"] = time.time()
            self._log(f"FALHA: {a.parada} {', '.join(a.brokers)} por {self.janela_s:.0f}s")
            rc, out = await amb.compose_async(a.parada, *a.brokers)
            self.marcos["falha_efetivada"] = time.time()
            if rc != 0:
                self._log(f"falha ao injetar: {out}")
            await asyncio.sleep(max(0.0, self.janela_s - (time.time() - self.marcos["falha_inicio"])))

            self.fase = "recuperacao"
            self.marcos["falha_fim"] = time.time()
            restaurar = "unpause" if a.parada == "pause" else "start"
            self._log(f"RESTAURANDO: {restaurar} {', '.join(a.brokers)}")
            rc, out = await amb.compose_async(restaurar, *a.brokers)
            self.marcos["restauracao_concluida"] = time.time()
            if rc != 0:
                self._log(f"falha ao restaurar: {out}")
            await asyncio.sleep(a.recuperacao)

            parar_carga.set()
            await t_carga
            self.gerador.fechar()
            self.marcos["carga_fim"] = time.time()
            self.fase = "estabilizacao"
            self._log("carga encerrada; aguardando convergência")
            await self._aguardar_convergencia()
            self.marcos["fim"] = time.time()
            parar_amostra.set()
            await t_amostra

        faltantes = await self.coletor.exportar(self.pasta)
        if faltantes:
            self.marcos["exportacoes_faltantes"] = faltantes
            self._log(f"não foi possível exportar {', '.join(faltantes)} (banco fora do ar)")
        await self.coletor.fechar()
        (self.pasta / "marcos.json").write_text(json.dumps(self.marcos, indent=2))
        self._salvar_logs()
        return analisar(self.pasta)

    async def _aguardar_convergencia(self) -> None:
        a = self.args
        inicio = time.time()
        while time.time() - inicio < a.estabilizacao_max:
            await asyncio.sleep(a.intervalo)
            u = self.ultima
            if u.get("gap") == 0 and u.get("divergencia") == 0:
                self._log("origem e destino convergiram")
                return
            ultimo = self.coletor.ultimo_recebimento
            parado = ultimo is None or time.time() - ultimo.timestamp() > a.sem_progresso
            if parado and time.time() - inicio > a.sem_progresso:
                self._log(f"sem mensagens novas há {a.sem_progresso:.0f}s; encerrando com divergência "
                          f"(gap={u.get('gap')}, divergência={u.get('divergencia')})")
                return
            if self.modo == "cdc" and a.reiniciar_conector_falho and u.get("conector_estado") == "FAILED":
                await self._intervir()
        self._log("limite de estabilização atingido")

    async def _intervir(self) -> None:
        if "intervencao_conector" not in self.marcos:
            self.marcos["intervencao_conector"] = time.time()
            self._log("tarefa do conector em FAILED: reiniciando (intervenção registrada)")
        await amb.reiniciar_tarefas_falhas(self.sessao)

    async def _amostrador(self, parar: asyncio.Event) -> None:
        with open(self.pasta / "serie_temporal.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CAMPOS_SERIE)
            w.writeheader()
            while not parar.is_set():
                t0 = time.time()
                try:
                    linha = await self._amostra()
                    w.writerow(linha)
                    f.flush()
                    self.ultima = linha
                    self._log(f"{linha['fase']:<13} ok={linha['api_sucesso']:<5} erro={linha['api_erro']:<4} "
                              f"p99={linha['api_p99_ms']}ms gap={linha['gap']:<6} div={linha['divergencia']:<6} "
                              f"{self._mb('wal', linha['origem_wal_bytes'])}"
                              f"{self._mb('slot', linha['slot_retido_bytes']) if self.modo == 'cdc' else ''}"
                              f"{'' if linha['origem_disponivel'] else 'ORIGEM FORA DO AR '}"
                              f"{'conector=' + linha['conector_estado'] if self.modo == 'cdc' else 'falhas_pub=' + str(linha['publicacao_falhas_total'])}")
                    if self.modo == "cdc" and self.args.reiniciar_conector_falho and \
                            self.fase == "recuperacao" and linha["conector_estado"] == "FAILED":
                        await self._intervir()
                except Exception as e:
                    self._log(f"erro na amostragem: {type(e).__name__}: {e}")
                try:
                    await asyncio.wait_for(parar.wait(), max(0.0, self.args.intervalo - (time.time() - t0)))
                except asyncio.TimeoutError:
                    pass

    async def _amostra(self) -> dict:
        j = self.gerador.snapshot()
        bd = await self.coletor.amostrar()
        met = await amb.ler_metricas(self.sessao, amb.API_URL)
        gap = len(self.gerador.commitadas - self.coletor.recebidos)
        linha = {
            "t": f"{time.time():.3f}", "instante": datetime.now().strftime("%H:%M:%S"), "fase": self.fase,
            "api_enviadas": j.enviadas, "api_sucesso": j.sucesso, "api_erro": j.erro,
            "api_nao_enviadas": j.nao_enviadas,
            "api_p99_ms": round(statistics.quantiles(j.latencias_ms, n=100)[98], 1) if len(j.latencias_ms) > 1 else "",
            "em_voo": self.gerador.em_voo, "commitadas_total": len(self.gerador.commitadas),
            "destino_mensagens_total": bd["destino_mensagens_total"],
            "destino_correlacoes": bd["destino_correlacoes_distintas"], "gap": gap,
            "divergencia": await self.coletor.divergencia(),
            "publicacao_ok_total": met.get("events_published_total", ""),
            "publicacao_falhas_total": met.get("events_publish_failed_total", ""),
            "conector_estado": await amb.estado_conector(self.sessao) if self.modo == "cdc" else "",
        }
        linha.update({k: bd[k] for k in ("origem_disponivel", "origem_db_bytes", "origem_wal_bytes", "origem_pedidos_tocados",
                                          "slot_ativo", "slot_retido_bytes", "slot_atraso_bytes")})
        return linha

    @staticmethod
    def _mb(rotulo: str, v) -> str:
        return f"{rotulo}={int(v) >> 20}MB " if v not in ("", None) else ""

    def _salvar_logs(self) -> None:
        logs = self.pasta / "logs"
        logs.mkdir(exist_ok=True)
        servicos = [amb.MODOS[self.modo]["order"], amb.MODOS[self.modo]["consumidor"], *amb.BROKERS]
        if self.modo == "cdc":
            servicos.append("kafka-connect")
        for s in servicos:
            r = amb.compose("logs", "--no-color", "--timestamps", s, check=False, capturar=True, timeout=120)
            (logs / f"{s}.log").write_text(r.stdout + r.stderr)

    def _log(self, msg: str) -> None:
        linha = f"[{datetime.now():%H:%M:%S}] [{self.modo} {self.janela_s:.0f}s] {msg}"
        with open(self.pasta / "execucao.log", "a") as f:
            f.write(linha + "\n")
        print(linha, flush=True)


def fracao_seed() -> float:
    with psycopg.connect(amb.ORIGEM_DSN) as c:
        r = c.execute("SELECT seed_fraction, synthetic FROM seed_info").fetchone()
    if r is None:
        raise SystemExit("seed_info vazio: a carga inicial não rodou")
    if r[1]:
        print("ATENÇÃO: o banco foi carregado com a amostra SINTÉTICA, não com o dataset Olist real")
    return float(r[0])


CAMPOS_CONSOLIDADO = ["execucao", "modo", "janela_s", "repeticao", "commitadas", "perda_eventos", "perda_pct",
                      "perda_mensagens", "duplicacao", "violacao_ordem", "div_ausentes", "div_versao",
                      "api_sucesso_falha", "api_p99_falha_ms", "api_p99_regime_ms", "retomada_emissao_s",
                      "retomada_entrega_s", "convergencia_s", "wal_max_falha_mb", "slot_max_falha_mb"]


def consolidar(arquivo: Path, r: dict, rep: int) -> None:
    novo = not arquivo.exists()
    i, t, api, col = r["integridade"], r["tempos"], r["api"], r["efeito_colateral"]
    mb = lambda v: round(v / 2**20, 1) if v is not None else ""
    with open(arquivo, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CAMPOS_CONSOLIDADO)
        if novo:
            w.writeheader()
        w.writerow({
            "execucao": r["execucao"], "modo": r["modo"], "janela_s": r["duracao_falha_s"], "repeticao": rep,
            "commitadas": i["operacoes_commitadas"], "perda_eventos": i["perda_eventos"],
            "perda_pct": i["perda_eventos_pct"], "perda_mensagens": i["perda_mensagens"],
            "duplicacao": i["duplicacao_mensagens"], "violacao_ordem": i["violacao_ordem"],
            "div_ausentes": i["divergencia_final"].get("ausentes_no_destino", ""),
            "div_versao": i["divergencia_final"].get("versao_ou_status_diferente", ""),
            "api_sucesso_falha": api["falha"]["taxa_sucesso"], "api_p99_falha_ms": api["falha"]["latencia_p99_ms"],
            "api_p99_regime_ms": api["regime"]["latencia_p99_ms"],
            "retomada_emissao_s": t.get("retomada_emissao_s"), "retomada_entrega_s": t.get("retomada_entrega_s"),
            "convergencia_s": t.get("convergencia_s"),
            "wal_max_falha_mb": mb(col["wal_max_bytes"]["falha"]),
            "slot_max_falha_mb": mb(col.get("slot_retido_max_bytes", {}).get("falha")),
        })


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--modo", required=True, choices=["domain-events", "cdc", "ambos"])
    p.add_argument("--janelas", default="5,15,30,60", help="durações da falha em minutos (padrão: 5,15,30,60)")
    p.add_argument("--janela-segundos", help="durações em segundos; substitui --janelas (ensaios rápidos)")
    p.add_argument("--repeticoes", type=int, default=1)
    p.add_argument("--taxa", type=float, default=100, help="operações por segundo (padrão: 100)")
    p.add_argument("--regime", type=float, default=120, help="segundos de carga antes da falha")
    p.add_argument("--recuperacao", type=float, default=120, help="segundos de carga depois da falha")
    p.add_argument("--estabilizacao-max", type=float, default=900, help="limite de espera pela convergência (s)")
    p.add_argument("--sem-progresso", type=float, default=60,
                   help="encerra a estabilização se nenhuma mensagem chegar por este tempo (s)")
    p.add_argument("--intervalo", type=float, default=5, help="intervalo de amostragem (s)")
    p.add_argument("--brokers", default=",".join(amb.BROKERS),
                   help="brokers derrubados (padrão: todos; ex.: kafka-2,kafka-3 deixa partições sem quórum)")
    p.add_argument("--parada", choices=["stop", "kill", "pause"], default="stop",
                   help="stop = SIGTERM, kill = SIGKILL, pause = processo congelado e inalcançável")
    p.add_argument("--reiniciar-conector-falho", action="store_true",
                   help="no CDC, reinicia a tarefa do Debezium se ela terminar em FAILED (registra a intervenção)")
    p.add_argument("--max-em-voo", type=int, default=2000)
    p.add_argument("--sem-reset", action="store_true", help="não reinicializa o ambiente antes de cada execução")
    p.add_argument("--build", action="store_true", help="reconstrói a imagem dos serviços ao subir")
    p.add_argument("--saida", default="resultados/cenario1")
    a = p.parse_args()
    a.brokers = [b.strip() for b in a.brokers.split(",") if b.strip()]
    janelas = [float(x) for x in a.janela_segundos.split(",")] if a.janela_segundos else \
        [float(x) * 60 for x in a.janelas.split(",")]
    modos = ["domain-events", "cdc"] if a.modo == "ambos" else [a.modo]

    saida = amb.RAIZ / a.saida
    saida.mkdir(parents=True, exist_ok=True)
    pedidos = None
    for rep in range(1, a.repeticoes + 1):
        for modo in modos:
            for janela in janelas:
                nome = f"{datetime.now():%Y%m%d-%H%M%S}_{modo}_janela{janela:.0f}s_rep{rep}"
                pasta = saida / nome
                pasta.mkdir()
                if not a.sem_reset:
                    amb.reiniciar_ambiente(modo, build=a.build)
                    a.build = False
                # Com reset, os pedidos do Olist entram com o order_id original; sem reset, usa-se
                # uma rodada de clones com ids novos para não colidir com execuções anteriores.
                a.rodada_inicial = int(time.time()) if a.sem_reset else 0
                if pedidos is None:
                    pedidos = carregar_reproducao(fracao_seed())
                    print(f"{len(pedidos)} pedidos no conjunto de reprodução")
                r = asyncio.run(Execucao(modo, janela, a, pasta).rodar(pedidos))
                imprimir(r)
                consolidar(saida / "consolidado.csv", r, rep)
    print(f"\nResultados em {saida}")


if __name__ == "__main__":
    main()
