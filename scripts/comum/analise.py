"""Cálculo das métricas de uma execução a partir dos arquivos brutos gravados na pasta dela.

Uso: python -m scripts.comum.analise resultados/<execucao>

Arquivos lidos: marcos.json, ledger.csv, serie_temporal.csv, origem_pedidos.csv,
destino_pedidos.csv, destino_mensagens.csv e os *_por_pedido.csv.
Grava resumo.json na mesma pasta.

Definições (Seção 4.5 e Cenário 1):
- operação commitada: resposta 2xx da API (o serviço só responde depois do commit);
  criações sem resposta (timeout) cujo pedido existe na origem também contam.
- perda de eventos: operação commitada da qual nenhuma mensagem chegou ao destino.
- perda de mensagens: mensagens esperadas e não recebidas. No Domain Events cada operação
  gera 1 mensagem; no CDC a criação gera 1 + itens + pagamentos (uma por linha).
- duplicação: mensagens extras para a mesma (correlation_id, entidade, chave).
- violação de ordem: mensagem de pedido com versão menor que outra já recebida do mesmo pedido.
- divergência final: pedidos tocados cuja (versão, status) difere entre origem e destino,
  ou cuja contagem de itens/pagamentos difere.

Seções extras, conforme o cenário gravado em marcos.json:
- Cenário 2 (promoção de réplica): transações confirmadas ao cliente que a réplica promovida
  não tem, eventos órfãos (dessas transações, as que chegaram ao destino) e divergência de
  conteúdo. As transações perdidas na promoção saem do oráculo geral, porque a origem não as
  tem mais; elas são contadas à parte.
- Cenário 3 (reinícios repetidos): operações sem resposta (o serviço morreu no meio) são
  resolvidas pela versão do pedido na origem, que sobe 1 a cada commit.
- Cenário 5 (queda de consumidor): rebalanceamentos e tempo fora do estado Stable, lidos de
  grupo.csv, e expulsões por max.poll.interval.
"""

from __future__ import annotations

import csv
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


def _ts(v: str) -> float | None:
    return datetime.fromisoformat(v).timestamp() if v else None


def _ler(p: Path) -> list[dict]:
    if not p.exists():
        return []
    with open(p, newline="") as f:
        return list(csv.DictReader(f))


def _pct(valores: list[float], q: float) -> float | None:
    if not valores:
        return None
    if len(valores) == 1:
        return round(valores[0], 2)
    return round(statistics.quantiles(valores, n=100, method="inclusive")[int(q) - 1], 2)


def _fase(t: float, m: dict) -> str:
    if m.get("falha_inicio") is None or t < m["falha_inicio"]:
        return "regime"
    if m.get("falha_fim") is None or t < m["falha_fim"]:
        return "falha"
    return "recuperacao"


def analisar(pasta: Path) -> dict:
    m = json.loads((pasta / "marcos.json").read_text())
    modo = m["modo"]
    ledger = _ler(pasta / "ledger.csv")
    serie = _ler(pasta / "serie_temporal.csv")
    msgs = _ler(pasta / "destino_mensagens.csv")
    origem = {r["order_id"]: r for r in _ler(pasta / "origem_pedidos.csv")}
    destino = {r["order_id"]: r for r in _ler(pasta / "destino_pedidos.csv")}

    # ------------------------------------------------------------------ API por fase
    api: dict[str, dict] = {}
    por_fase: dict[str, list[dict]] = defaultdict(list)
    for r in ledger:
        por_fase[_fase(float(r["t_envio"]), m)].append(r)
    for fase in ("regime", "falha", "recuperacao"):
        rs = por_fase.get(fase, [])
        ok = [r for r in rs if r["http_status"].isdigit() and 200 <= int(r["http_status"]) < 300]
        lat = [float(r["latencia_ms"]) for r in rs]
        api[fase] = {
            "operacoes": len(rs),
            "sucesso": len(ok),
            "taxa_sucesso": round(len(ok) / len(rs), 4) if rs else None,
            "latencia_p50_ms": _pct(lat, 50),
            "latencia_p99_ms": _pct(lat, 99),
        }

    # ------------------------------------------------------------------ oráculo
    commitadas: dict[str, dict] = {}
    ambiguas = 0
    for r in ledger:
        st = int(r["http_status"]) if r["http_status"].isdigit() else 0
        if 200 <= st < 300:
            commitadas[r["correlation_id"]] = r
        elif st == 0:
            if r["operacao"] == "create" and r["order_id"] in origem:
                commitadas[r["correlation_id"]] = r
            else:
                ambiguas += 1

    extras: dict = {}
    if m.get("resolver_ambiguas"):
        resolvidas, ambiguas, extras["ambiguas"] = _resolver_ambiguas(ledger, origem)
        commitadas.update(resolvidas)
    perdidas_promocao: dict[str, dict] = {}
    if m.get("cenario") == 2:
        perdidas_promocao, extras["promocao"] = _promocao(pasta, m, ledger, msgs, origem, destino, serie)
        for corr in perdidas_promocao:
            commitadas.pop(corr, None)
    if m.get("cenario") == 3:
        ciclos = _ler(pasta / "ciclos.csv")
        prontos = [float(c["tempo_ate_pronto_s"]) for c in ciclos if c["tempo_ate_pronto_s"]]
        extras["reinicios"] = {
            "componente": m.get("parametros", {}).get("componente"),
            "ciclos": len(ciclos),
            "ciclos_sem_voltar": len(ciclos) - len(prontos),
            "tempo_ate_pronto_medio_s": round(statistics.mean(prontos), 2) if prontos else None,
            "tempo_fora_total_s": round(sum(prontos), 1),
        }
    if m.get("cenario") == 5:
        extras["grupo"] = _grupo(pasta, m, serie)

    por_corr: dict[str, Counter] = defaultdict(Counter)
    for x in msgs:
        if x["correlation_id"]:
            por_corr[x["correlation_id"]][(x["entity"], x["entity_key"])] += 1

    def esperadas(op: dict) -> int:
        if modo == "cdc" and op["operacao"] == "create":
            return 1 + int(op["n_itens"]) + int(op["n_pagamentos"])
        return 1

    perdidas_por_fase: Counter = Counter()
    commitadas_por_fase: Counter = Counter()
    perda_msgs = esperadas_total = 0
    for corr, op in commitadas.items():
        fase = _fase(float(op["t_envio"]), m)
        commitadas_por_fase[fase] += 1
        recebidas = por_corr.get(corr)
        if not recebidas:
            perdidas_por_fase[fase] += 1
        e = esperadas(op)
        esperadas_total += e
        perda_msgs += max(0, e - (len(recebidas) if recebidas else 0))

    duplicadas = sum(n - 1 for c in por_corr.values() for n in c.values() if n > 1)

    violacoes = 0
    maior: dict[str, int] = {}
    for x in msgs:  # já ordenadas por id (ordem de chegada)
        if x["entity"] != "order" or not x["version"]:
            continue
        v = int(x["version"])
        if v < maior.get(x["order_id"], 0):
            violacoes += 1
        maior[x["order_id"]] = max(v, maior.get(x["order_id"], 0))

    # ------------------------------------------------------------------ divergência final
    ausentes = sum(1 for k in origem if k not in destino)
    diferentes = sum(1 for k, o in origem.items()
                     if k in destino and (o["version"], o["order_status"]) !=
                     (destino[k]["version"], destino[k]["order_status"]))
    filhos_div = {}
    for tabela in ("order_items", "order_payments"):
        o = {r["order_id"]: r["linhas"] for r in _ler(pasta / f"origem_{tabela}_por_pedido.csv") if r["order_id"] in origem}
        d = {r["order_id"]: r["linhas"] for r in _ler(pasta / f"destino_{tabela}_por_pedido.csv")}
        filhos_div[tabela] = sum(1 for k, n in o.items() if d.get(k) != n)

    # ------------------------------------------------------------------ tempos
    fi, ff = m.get("falha_inicio"), m.get("falha_fim")
    tempos: dict[str, float | None] = {}
    if ff is not None:
        emit = [t for x in msgs if (t := _ts(x["kafka_timestamp"])) is not None and t >= ff]
        entr = [t for x in msgs if (t := _ts(x["received_at"])) is not None and t >= ff]
        tempos["retomada_emissao_s"] = round(min(emit) - ff, 2) if emit else None
        tempos["retomada_entrega_s"] = round(min(entr) - ff, 2) if entr else None
        conv = [float(s["t"]) for s in serie
                if float(s["t"]) >= ff and s.get("divergencia") not in ("", None) and int(s["divergencia"]) == 0]
        tempos["convergencia_s"] = round(conv[0] - ff, 2) if conv else None
    if fi is not None:
        regime = [s for s in serie if float(s["t"]) < fi]
        falha = [s for s in serie if float(s["t"]) >= fi]
        base_gap = max((int(s["gap"]) for s in regime), default=0)
        tempos["deteccao_gap_s"] = next((round(float(s["t"]) - fi, 2) for s in falha
                                         if int(s["gap"]) > max(base_gap * 2, 10)), None)
        if modo == "domain-events":
            base = float(regime[-1]["publicacao_falhas_total"] or 0) if regime else 0.0
            tempos["deteccao_erro_publicacao_s"] = next(
                (round(float(s["t"]) - fi, 2) for s in falha
                 if s["publicacao_falhas_total"] and float(s["publicacao_falhas_total"]) > base), None)
        else:
            tempos["deteccao_conector_s"] = next(
                (round(float(s["t"]) - fi, 2) for s in falha if s["conector_estado"] not in ("RUNNING", "")), None)
            base_slot = max((int(s["slot_atraso_bytes"] or 0) for s in regime), default=0)
            tempos["deteccao_slot_s"] = next(
                (round(float(s["t"]) - fi, 2) for s in falha
                 if s["slot_atraso_bytes"] and int(s["slot_atraso_bytes"]) > max(2 * base_slot, 16 << 20)), None)

        # Momento em que o banco de origem deixou de responder (ex.: disco esgotado pelo WAL retido).
        tempos["origem_indisponivel_s"] = next(
            (round(float(s["t"]) - fi, 2) for s in falha if s.get("origem_disponivel") == "0"), None)

    # ------------------------------------------------------------------ efeito colateral
    def maximo(campo: str, fase: str) -> int | None:
        vals = [int(s[campo]) for s in serie if s.get(campo) not in ("", None) and _fase(float(s["t"]), m) == fase]
        return max(vals) if vals else None

    colateral = {
        "wal_max_bytes": {f: maximo("origem_wal_bytes", f) for f in ("regime", "falha", "recuperacao")},
        "db_max_bytes": {f: maximo("origem_db_bytes", f) for f in ("regime", "falha", "recuperacao")},
    }
    if modo == "cdc":
        colateral["slot_retido_max_bytes"] = {f: maximo("slot_retido_bytes", f) for f in ("regime", "falha", "recuperacao")}
        estados = sorted({s["conector_estado"] for s in serie if s.get("conector_estado")})
        colateral["estados_conector_observados"] = estados

    divergencia_final = {
        "pedidos_tocados": len(origem),
        "ausentes_no_destino": ausentes,
        "versao_ou_status_diferente": diferentes,
        "contagem_itens_diferente": filhos_div["order_items"],
        "contagem_pagamentos_diferente": filhos_div["order_payments"],
    }
    if "origem_pedidos.csv" in m.get("exportacoes_faltantes", []):
        # Sem a origem não há com o que comparar (ex.: banco caiu por disco cheio).
        divergencia_final = {"indisponivel": "banco de origem fora do ar ao fim da execução"}
    if m.get("exportacoes_faltantes"):
        colateral["exportacoes_faltantes"] = m["exportacoes_faltantes"]
    total_commit = len(commitadas)
    resumo = {
        "execucao": pasta.name,
        "modo": modo,
        "parametros": m.get("parametros", {}),
        "duracao_falha_s": round(ff - fi, 1) if fi is not None and ff is not None else None,
        "api": api,
        "integridade": {
            "operacoes_commitadas": total_commit,
            "operacoes_commitadas_por_fase": dict(commitadas_por_fase),
            "operacoes_ambiguas_excluidas": ambiguas,
            "perda_eventos": sum(perdidas_por_fase.values()),
            "perda_eventos_por_fase": dict(perdidas_por_fase),
            "perda_eventos_pct": round(100 * sum(perdidas_por_fase.values()) / total_commit, 4) if total_commit else None,
            "mensagens_esperadas": esperadas_total,
            "mensagens_recebidas": len(msgs),
            "perda_mensagens": perda_msgs,
            "duplicacao_mensagens": duplicadas,
            "violacao_ordem": violacoes,
            "divergencia_final": divergencia_final,
        },
        "tempos": tempos,
        "efeito_colateral": colateral,
        **extras,
    }
    (pasta / "resumo.json").write_text(json.dumps(resumo, indent=2, ensure_ascii=False))
    return resumo


def _ok(r: dict) -> bool:
    return r["http_status"].isdigit() and 200 <= int(r["http_status"]) < 300


def _resolver_ambiguas(ledger: list[dict], origem: dict) -> tuple[dict[str, dict], int, dict]:
    """Decide, pela versão, se as operações sem resposta (status 0) chegaram a ser commitadas.

    As operações de um pedido são sequenciais e cada commit soma 1 à versão. Entre duas
    respostas 2xx com versões v1 e v2 houve v2 - v1 - 1 commits sem resposta; depois da
    última, a versão final da origem diz quantos houve. Se esse número bate com a quantidade
    de operações sem resposta do trecho (ou é zero), todas foram (ou nenhuma foi) commitadas.
    Caso contrário o trecho fica sem decisão e sai do oráculo."""
    por_pedido: dict[str, list[dict]] = defaultdict(list)
    for r in ledger:
        por_pedido[r["order_id"]].append(r)
    commitadas: dict[str, dict] = {}
    cont = Counter()
    for oid, ops in por_pedido.items():
        ops.sort(key=lambda r: float(r["t_envio"]))
        anterior, trecho = 0, []

        def fechar(proxima: int) -> None:
            if not trecho:
                return
            n = proxima - anterior - 1
            if n == len(trecho):
                cont["commitadas"] += n
                commitadas.update({r["correlation_id"]: r for r in trecho})
            elif n <= 0:
                cont["nao_commitadas"] += len(trecho)
            else:
                cont["sem_decisao"] += len(trecho)

        for r in ops:
            if _ok(r) and r["versao"]:
                fechar(int(r["versao"]))
                trecho, anterior = [], int(r["versao"])
            elif r["http_status"] in ("", "0"):
                trecho.append(r)
        fechar(int(origem[oid]["version"]) + 1 if oid in origem else 1)
    return commitadas, cont["sem_decisao"], {
        "sem_resposta": sum(cont.values()), "commitadas": cont["commitadas"],
        "nao_commitadas": cont["nao_commitadas"], "sem_decisao": cont["sem_decisao"]}


def _lsn(v: str | None) -> int | None:
    if not v:
        return None
    a, _, b = v.partition("/")
    return (int(a, 16) << 32) + int(b, 16)


def _promocao(pasta: Path, m: dict, ledger: list[dict], msgs: list[dict], origem: dict, destino: dict,
              serie: list[dict]) -> tuple[dict[str, dict], dict]:
    """Cenário 2: o que a promoção da réplica apagou e o que disso chegou ao destino."""
    retrato = {r["order_id"]: r for r in _ler(pasta / "origem_pos_promocao.csv")}
    queda = m.get("primario_derrubado") or float("inf")
    # Antes de a réplica assumir o endereço, nenhuma escrita chega a ela: toda resposta 2xx
    # recebida até ali veio de um commit no primário antigo. Respostas posteriores podem ser de
    # requisições que esperaram a conexão e commitaram já no novo primário.
    troca = m.get("endereco_trocado") or m.get("falha_fim") or float("inf")
    perdidas: dict[str, dict] = {}
    for r in ledger:
        if not _ok(r) or not r["versao"] or float(r["t_resposta"]) > troca:
            continue
        s = retrato.get(r["order_id"])
        if s is None or int(s["version"]) < int(r["versao"]):
            perdidas[r["correlation_id"]] = r
    recebidas = {x["correlation_id"] for x in msgs if x["correlation_id"]}
    orfaos = [c for c in perdidas if c in recebidas]

    so_destino = [k for k in destino if k not in origem]
    adiantado = sum(1 for k, d in destino.items() if k in origem and int(d["version"]) > int(origem[k]["version"]))
    conflito = sum(1 for k, d in destino.items() if k in origem and d["version"] == origem[k]["version"]
                   and d["last_correlation_id"] != origem[k]["last_correlation_id"])

    ref = m.get("promocao_concluida") or queda
    ausente = recriado = None
    for x in serie:
        t = float(x["t"])
        if t < queda or x.get("origem_disponivel") != "1":
            continue
        tem_slot = x.get("slot_ativo") not in ("", None)
        if ausente is None and not tem_slot:
            ausente = round(t - ref, 2)
        elif ausente is not None and tem_slot and recriado is None:
            recriado = round(t - ref, 2)
    antes, depois = _lsn(m.get("lsn_primario_na_queda")), _lsn(m.get("lsn_replica_na_promocao"))
    return perdidas, {
        "transacoes_perdidas_na_promocao": len(perdidas),
        "criacoes_perdidas": sum(1 for r in perdidas.values() if r["operacao"] == "create"),
        "eventos_orfaos": len(orfaos),
        "wal_nao_replicado_bytes": antes - depois if antes is not None and depois is not None else None,
        "conteudo_final": {
            "pedidos_so_no_destino": len(so_destino),
            "destino_com_versao_maior": adiantado,
            "mesma_versao_conteudo_diferente": conflito,
        },
        "slot_ausente_apos_promocao_s": ausente if m["modo"] == "cdc" else None,
        "slot_recriado_apos_promocao_s": recriado if m["modo"] == "cdc" else None,
    }


def _grupo(pasta: Path, m: dict, serie: list[dict]) -> dict:
    """Cenário 5: rebalanceamentos e tempo fora do estado Stable por fase, a partir de grupo.csv."""
    linhas = [r for r in _ler(pasta / "grupo.csv") if r["estado"]]
    fases = ("regime", "falha", "recuperacao", "estabilizacao")
    reb, nao_estavel = Counter(), Counter()
    for a, b in zip(linhas, linhas[1:]):
        t = float(a["t"])
        fase = _fase(t, m) if t < (m.get("carga_fim") or float("inf")) else "estabilizacao"
        if a["estado"] != "Stable":
            nao_estavel[fase] += float(b["t"]) - t
        if a["estado"] == "Stable" and b["estado"] != "Stable":
            reb[_fase(float(b["t"]), m) if float(b["t"]) < (m.get("carga_fim") or float("inf")) else "estabilizacao"] += 1

    def delta(campo: str, fase: str) -> int | None:
        vals = [float(s[campo]) for s in serie if s.get(campo) not in ("", None) and s["fase"] == fase]
        return int(max(vals) - min(vals)) if vals else None

    return {
        "rebalanceamentos": {f: reb[f] for f in fases},
        "tempo_nao_estavel_s": {f: round(nao_estavel[f], 1) for f in fases},
        "membros_min": min((int(r["membros"]) for r in linhas), default=None),
        "membros_max": max((int(r["membros"]) for r in linhas), default=None),
        "expulsoes_max_poll": {f: delta("consumidor_expulsoes", f) for f in fases},
    }


def imprimir(r: dict) -> None:
    i = r["integridade"]
    print(f"\n=== {r['execucao']} ({r['modo']}) — falha de {r['duracao_falha_s']} s ===")
    for fase, a in r["api"].items():
        print(f"  API {fase:<12} ops={a['operacoes']:<7} sucesso={a['taxa_sucesso']}  "
              f"p50={a['latencia_p50_ms']} ms  p99={a['latencia_p99_ms']} ms")
    print(f"  commitadas={i['operacoes_commitadas']}  perda de eventos={i['perda_eventos']} "
          f"({i['perda_eventos_pct']}%) por fase={i['perda_eventos_por_fase']}")
    print(f"  mensagens esperadas={i['mensagens_esperadas']} recebidas={i['mensagens_recebidas']} "
          f"perdidas={i['perda_mensagens']} duplicadas={i['duplicacao_mensagens']} fora de ordem={i['violacao_ordem']}")
    print(f"  divergência final={i['divergencia_final']}")
    print(f"  tempos={r['tempos']}")
    print(f"  efeito colateral={r['efeito_colateral']}")
    for chave in ("ambiguas", "reinicios", "promocao", "grupo"):
        if chave in r:
            print(f"  {chave}={r[chave]}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    imprimir(analisar(Path(sys.argv[1])))
