# TCC: protótipo experimental 1 (Olist)

Protótipos do experimento que compara **Domain Events** e **Change Data Capture** como modelos de
propagação de mudanças entre microsserviços sob falhas de disponibilidade. Este repositório
usa o **Dataset 1**, o [Brazilian E-Commerce da Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce),
e traz os scripts de carga e de injeção de falha dos **Cenários 1 (fila indisponível), 2 (promoção de
réplica), 3 (reinícios repetidos) e 5 (queda de consumidor)**. O Cenário 4 (compensação) ainda falta e
exige pelo menos três serviços.

| | Software A: Domain Events | Software B: Change Data Capture |
|---|---|---|
| Quem propaga | o próprio `order-service`, logo após o commit | Debezium (Kafka Connect) lendo o WAL do PostgreSQL |
| Outbox | **não** (janela de dual write exposta de propósito) | não se aplica |
| Mensagens por operação | 1 evento por operação de negócio | 1 mensagem por linha alterada |
| Subir | `docker compose up backend-domain-events` | `docker compose up backend-change-data-capture` |

Todo o resto é idêntico nos dois: o mesmo binário Go (o modo vem de `PROPAGATION_MODE`), o mesmo
esquema, os mesmos endpoints, o mesmo consumidor, os mesmos bancos e o mesmo cluster Kafka.

## Arquitetura

```
                     Software A (domain-events)
                  ┌──────── publica após o commit ─────────┐
                  │                                        ▼
cliente ──HTTP──▶ order-service ──▶ postgres-origem     Kafka (3 brokers, KRaft) ──▶ shipping-service ──▶ postgres-destino
 (scripts/carga)  (Go, :8080)       (wal_level=logical)    ▲                        (Go, :8081)
                                          │                │
                                          └── WAL ──▶ Debezium/Kafka Connect (:8083)
                                               Software B (cdc)
```

- **order-service** (origem): `POST /orders`, `PATCH /orders/{id}/status`, `GET /orders/{id}`,
  `GET /healthz`, `GET /metrics`. Cada requisição aceita o cabeçalho `X-Correlation-ID`, gravado na
  linha (`last_correlation_id` / `correlation_id`) e propagado até o destino.
- **shipping-service** (destino): consome a fila, materializa pedidos, itens e pagamentos e registra
  **cada mensagem recebida** em `received_events`. É dali que saem perda, duplicação e ordem.
- **Kafka**: 3 brokers `apache/kafka:3.8.0` em modo KRaft, tópicos com 6 partições, fator de
  replicação 3 e `min.insync.replicas=2`.
- **PostgreSQL 16**: um banco por serviço. `wal_level=logical` nos dois modos, para que a origem seja
  idêntica; o slot de replicação só existe no Software B.

## Pré-requisitos

- Docker com Compose v2.20 ou mais novo (`docker compose version`). Reserve uns 6 GB de RAM.
- Python 3.10+ para os scripts:

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r scripts/requirements.txt
```

## Dados

O dataset exige login no Kaggle, por isso não está no repositório. Os CSVs vão em `data/olist/`:

1. Baixe o zip em https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce (botão *Download*),
   ou use a CLI do Kaggle: `kaggle datasets download -d olistbr/brazilian-ecommerce -p data/olist --unzip`.
2. Confira que existem, em `data/olist/`: `olist_customers_dataset.csv`, `olist_sellers_dataset.csv`,
   `olist_products_dataset.csv`, `olist_orders_dataset.csv`, `olist_order_items_dataset.csv` e
   `olist_order_payments_dataset.csv` (os demais arquivos do zip são ignorados).

**Carga inicial e carga de teste.** Os pedidos são ordenados pela data de compra. Os primeiros 70%
(`SEED_FRACTION`) são gravados direto nas duas bases pelo serviço `seed` quando o ambiente sobe, para
que os testes não comecem com bancos vazios. Os 30% restantes são reproduzidos pela API durante os
testes, cada pedido como uma criação seguida das transições do seu ciclo de vida no Olist
(`approved`, `shipped`, `delivered` ou o status final). Quando esse conjunto acaba numa carga longa,
ele é reaproveitado com `order_id` novos.

**Amostra sintética (só para validar o pipeline).** Sem o dataset, gere CSVs no mesmo formato:

```bash
python -m scripts.dados.gerar_amostra_sintetica --pedidos 20000 --saida data/olist-sintetico
echo "OLIST_DATA_DIR=./data/olist-sintetico" > .env
```

A pasta leva um marcador `_SINTETICO`; o seed registra isso em `seed_info.synthetic` e os scripts
avisam no terminal. Não use esses dados no TCC.

## Subindo os protótipos

```bash
docker compose up backend-domain-events          # Software A
docker compose up backend-change-data-capture    # Software B
```

Acrescente `-d` para liberar o terminal. O serviço `backend-*` só fica pronto depois que os bancos
estão saudáveis, a carga inicial terminou, os tópicos existem e (no B) o conector Debezium está
`RUNNING`.

Os dois usam as mesmas portas, então **derrube um antes de subir o outro**. O jeito mais simples de
alternar, que também zera as bases:

```bash
python -m scripts.comum.ambiente cdc             # ou domain-events
# equivalente a:
docker compose --profile domain-events --profile cdc down -v
docker compose up -d --wait backend-change-data-capture
```

Teste rápido da API:

```bash
curl -s localhost:8080/healthz
curl -s -X PATCH localhost:8080/orders/<order_id>/status -H 'Content-Type: application/json' \
     -d '{"order_status":"shipped"}'
curl -s localhost:8081/metrics
```

Portas no host: API `8080`, consumidor `8081`, Kafka Connect `8083` (só no B), banco de origem `5432`
e banco de destino `5433` (usuário, senha e base: `olist`).

## Painel

Uma página no navegador com o diagrama dos serviços, o estado de cada um ao vivo, a vazão entre
eles e botões para injetar falhas e disparar os scripts. Roda na máquina, com o venv ativo, fora
do Docker Compose (assim sobrevive ao `down -v` que os scripts fazem entre execuções):

```bash
python -m scripts.painel            # abre em http://localhost:8090
```

- **Diagrama.** Mostra o software que estiver no ar (A ou B), com cada contêiner em verde (no ar),
  âmbar (iniciando, não saudável ou sem responder ao `/healthz`), roxo (congelado com `pause`),
  vermelho (parado) ou cinza (não criado). As setas mostram a vazão lida dos `/metrics`, as falhas de
  publicação no A e, no B, o estado da tarefa do Debezium e o atraso do slot de replicação. Clicar
  num serviço abre os detalhes e os botões **Parar** (`stop`), **Matar** (`kill`), **Congelar**
  (`pause`), **Religar** e **Reiniciar**.
- **Injeção rápida.** Atalhos para derrubar a fila inteira, tirar o quórum (2 brokers), derrubar
  1 broker, o consumidor, um dos bancos ou o Debezium, com o tipo de parada escolhido; e **Restaurar
  tudo**, que religa o que estiver parado ou congelado. São os mesmos comandos `docker compose` do
  Cenário 1.
- **Scripts.** Sobe ou troca o backend, gera carga (constante, rajada ou represamento) e executa os
  Cenários 1, 2, 3 e 5 (e a calibração do 5) com os parâmetros do formulário, mostrando a saída do
  script. Enquanto um cenário ou a troca de backend rodam, os botões de falha ficam desativados para
  não interferir na execução.
- **Réplica da origem (Cenário 2).** Aparece à esquerda do banco de origem quando existe, como
  standby (com o atraso de replicação) ou como primário promovido. "Subir a réplica da origem" a
  cria a partir da origem atual, e "Failover da origem" faz o mesmo failover do script: SIGKILL no
  primário, promoção e troca do nome `postgres-origem` para a réplica. Depois do failover o painel
  não religa o primário antigo (seriam dois primários com o mesmo nome); suba o backend de novo.
- **Grupo de consumo (Cenário 5).** O shipping-service aparece como grupo, com as instâncias 1, 2 e 3
  e o estado do grupo lido do coordenador (estável, rebalanceando, vazio) com o número de membros.
  As instâncias 2 e 3 só existem no Cenário 5; "Subir as instâncias 2 e 3" as cria com a
  configuração padrão.
- **Gráfico e linha do tempo.** Vazão dos últimos 5 minutos, com faixas onde algum serviço estava
  fora do ar, e o registro de cada mudança de estado e de cada ação feita pelo painel.

O painel só escuta em `127.0.0.1`, porque controla o Docker. As falhas manuais servem para explorar
o comportamento; as medições do TCC continuam saindo dos scripts, que registram tudo em
`resultados/`.

## Testes de carga

Rodam contra o backend que estiver no ar (o modo é detectado sozinho):

```bash
# taxa constante: vazão, latência da API e atraso de propagação
python -m scripts.carga.teste_carga constante --taxa 200 --duracao 300

# rajada: N operações o mais rápido possível
python -m scripts.carga.teste_carga rajada --operacoes 50000 --concorrencia 300

# represamento: para o consumidor, enche a fila, religa e mede a drenagem do acúmulo
python -m scripts.carga.teste_carga represamento --operacoes 100000 --concorrencia 300
```

O `represamento` é o caso em que as filas têm muitas mensagens para sincronizar ao mesmo tempo. A
saída vai para `resultados/carga/<execução>/` (`ledger.csv`, exportações das bases,
`resumo.json` e `resumo_carga.json` com vazão da API, tempo de drenagem e vazão de consumo).

## Cenário 1: indisponibilidade da fila

```bash
# desenho completo (janelas de 5, 15, 30 e 60 min) para um software
python -m scripts.cenario1.fila_indisponivel --modo domain-events
python -m scripts.cenario1.fila_indisponivel --modo cdc

# os dois softwares, janelas escolhidas, 3 repetições
python -m scripts.cenario1.fila_indisponivel --modo ambos --janelas 5,15 --repeticoes 3

# ensaio rápido
python -m scripts.cenario1.fila_indisponivel --modo cdc --janela-segundos 60 --regime 30 --recuperacao 30
```

Cada execução:

1. reinicializa o ambiente (`down -v` e `up`), o que recarrega as bases, recria os tópicos e o slot;
2. **regime**: carga constante (`--taxa`, padrão 100 ops/s) por `--regime` segundos;
3. **falha**: `docker compose stop` nos três brokers pela duração da janela, com a carga continuando;
4. **recuperação**: sobe os brokers, mantém a carga por `--recuperacao` segundos, para a carga e
   acompanha até origem e destino convergirem ou até não chegar nada por `--sem-progresso` segundos.

Opções de injeção: `--parada kill` (SIGKILL), `--parada pause` (processo congelado e inalcançável) e
`--brokers kafka-2,kafka-3` (derruba só parte do cluster, deixando as partições sem quórum para
`acks=all`). No B, `--reiniciar-conector-falho` reinicia a tarefa do Debezium se ela terminar em
`FAILED` e registra a intervenção.

### O que sai em `resultados/cenario1/<execução>/`

| Arquivo | Conteúdo |
|---|---|
| `ledger.csv` | uma linha por requisição: horário, operação, `correlation_id`, status HTTP, latência |
| `serie_temporal.csv` | amostra a cada `--intervalo` s: sucesso e p99 da API, escritas commitadas, mensagens entregues, lacuna de propagação, divergência entre as bases, falhas de publicação (A), estado do conector (B), tamanho do banco, WAL e WAL retido pelo slot |
| `marcos.json` | instantes de início e fim de cada fase |
| `origem_*.csv`, `destino_*.csv` | exportação das bases ao final, para refazer a análise offline |
| `logs/` | logs dos serviços, brokers e Kafka Connect |
| `resumo.json` | as métricas da Seção 4.5 calculadas para a execução |

`resultados/cenario1/consolidado.csv` acumula uma linha por execução. Para recalcular uma execução:
`python -m scripts.comum.analise resultados/cenario1/<execução>`.

### Como as métricas são calculadas

- **Operação commitada**: resposta 2xx da API, que só responde depois do commit.
- **Perda de eventos**: operação commitada da qual nenhuma mensagem chegou ao destino (por
  `correlation_id`). Também é reportada a perda de mensagens, porque no CDC a criação de um pedido
  gera uma mensagem por linha (pedido, itens e pagamentos).
- **Duplicação**: mensagens repetidas para a mesma `(correlation_id, entidade, chave)`.
- **Violação de ordem**: mensagem de pedido com versão menor do que outra já recebida do mesmo pedido.
- **Divergência final**: pedidos tocados na execução cuja versão ou status diferem entre origem e
  destino, ou cuja quantidade de itens ou pagamentos difere.
- **Tempo até detecção**: primeira amostra após o início da falha com sinal observável. São
  reportados o sinal genérico (lacuna de propagação acima da linha de base), o erro de publicação no
  A e, no B, a tarefa do conector fora de `RUNNING` ou o atraso do slot acima de 16 MB.
- **Tempo até retomada**: da volta dos brokers até a primeira mensagem **emitida** (timestamp do
  Kafka) e até a primeira mensagem **entregue** ao destino.
- **Tempo até convergência**: da volta dos brokers até a primeira amostra com divergência zero.
- **Efeito colateral**: latência p99 e taxa de sucesso da API por fase; tamanho do banco, do WAL e
  do WAL retido pelo slot por fase.

### Disco limitado no banco de origem

No B, o slot de replicação segura o WAL enquanto a fila está fora. Para observar o ponto em que isso
esgota o disco e o banco passa a recusar escritas sem encher o disco da máquina, use a sobreposição
que coloca os dados da origem num tmpfs de tamanho fixo:

```bash
export COMPOSE_FILE=docker-compose.yml:docker-compose.disco-limitado.yml
export ORIGEM_DISCO_LIMITE=1g
python -m scripts.cenario1.fila_indisponivel --modo cdc --janelas 15 --taxa 400
```

## Cenário 2: promoção de réplica

```bash
python -m scripts.cenario2.promocao_replica --modo ambos
python -m scripts.cenario2.promocao_replica --modo cdc --atraso-replicacao 5 --atraso-promocao 5 --regime 30
```

O RDS é substituído por uma réplica física assíncrona da origem (`postgres-origem-replica`, porta
5434), criada com `pg_basebackup` depois da reinicialização do ambiente. Cada execução:

1. **regime** com primário e réplica saudáveis;
2. **falha**: a réplica sai da rede por `--atraso-replicacao` s (o que o primário commitar nesse
   intervalo não chega a ela), o primário leva SIGKILL e, `--atraso-promocao` s depois, a réplica é
   promovida e recebe o apelido de rede `postgres-origem`, como o endpoint de um banco gerenciado no
   failover. O order-service e o Debezium reconectam ao mesmo nome, sem mudança de configuração;
3. **recuperação** e **estabilização** como no Cenário 1, agora contra a réplica promovida.

Logo depois da promoção, antes de qualquer escrita nova, o script grava `origem_pos_promocao.csv` com
o que a réplica tem. O `resumo.json` ganha a seção `promocao`: transações confirmadas ao cliente que a
promoção apagou, quantas delas chegaram ao destino (**eventos órfãos**), pedidos que existem só no
destino, pedidos com versão maior no destino e pedidos com a mesma versão e conteúdo diferente (a
origem reusou o número de versão com outra mudança). As transações apagadas saem do oráculo geral de
perda, porque a origem não as tem mais. No B, também mede quando o slot sumiu e quando reapareceu.

Observado no ensaio na nuvem (amostra sintética, só para validar o pipeline): o Debezium 2.7 não
para quando o slot some; ele recria o slot no novo primário depois de uns 10 s de nova tentativa e
segue em `RUNNING`, e as mudanças commitadas nesse intervalo não são propagadas.

## Cenário 3: reinícios repetidos

```bash
python -m scripts.cenario3.reinicios_repetidos --modo ambos
python -m scripts.cenario3.reinicios_repetidos --modo cdc --duracao-falha 180 --tempo-no-ar 20
```

Durante `--duracao-falha` s (padrão 600), o componente de propagação leva SIGKILL, é religado, espera
ficar pronto (A: `/healthz` do order-service; B: tarefa do Debezium em `RUNNING`), fica no ar por
`--tempo-no-ar` s e morre de novo. No A o componente é o order-service; no B, o Kafka Connect. No B,
`--tempo-no-ar` precisa ser menor que `CDC_OFFSET_FLUSH_INTERVAL_MS` (60 s), para a morte vir antes da
gravação dos offsets. O religamento usa `docker start` direto, porque `docker compose start` rodaria o
seed e o kafka-init de novo e somaria uns 15 s a cada ciclo.

Saem `ciclos.csv` (morte, religamento e prontidão de cada ciclo) e, no `resumo.json`, as seções
`reinicios` e `ambiguas`. As requisições que estavam no meio quando o order-service morreu ficam sem
resposta; a análise decide se cada uma foi commitada pela versão do pedido na origem, que sobe 1 a
cada commit. Os trechos em que isso não é possível ficam em `sem_decisao` e saem do oráculo.

## Cenário 5: queda de consumidor

```bash
python -m scripts.cenario5.queda_consumidor --modo ambos --calibrar        # mede o lote cheio
python -m scripts.cenario5.queda_consumidor --modo ambos --max-poll-interval-ms 3000
```

O consumidor roda em 3 instâncias do mesmo grupo (portas 8081, 8084 e 8085), com 2 membros cada. O
kafka-go não tem `max.poll.interval.ms`, então o shipping-service ganhou um modo de leitura em lotes
que reproduz o cliente Java: busca até `CONSUMER_MAX_POLL_RECORDS` mensagens, processa o lote e só
então confirma os offsets; se o lote passar de `CONSUMER_MAX_POLL_INTERVAL`, o membro sai do grupo no
instante em que o limite vence, o que dispara um rebalanceamento, e o lote é reentregue. Com
`CONSUMER_MAX_POLL_INTERVAL=0` (padrão dos outros cenários) nada muda: uma mensagem por vez, como antes.

`--calibrar` para os consumidores por `--represamento` s sob carga, religa e mede o tempo médio de um
lote cheio durante a drenagem, sugerindo `--max-poll-interval-ms` 10% acima (grava
`calibracao.json`). Use um único valor nos dois softwares. Cada execução derruba a instância
`--instancia-derrubada` (padrão 2) por `--janela` s e a religa. O estado do grupo é lido a cada 0,5 s
do coordenador (`GET /group` de uma instância que não caiu) e gravado em `grupo.csv`; a seção `grupo`
do `resumo.json` traz rebalanceamentos, tempo fora de `Stable` e saídas por max.poll.interval por
fase. A drenagem é o tempo até convergência, e a latência da API por fase sai como nos outros cenários.

## Decisões de implementação que afetam as medições

Ficam registradas porque entram na descrição dos protótipos (Seção 4.2) e nas ameaças à validade:

- **Publicação síncrona no A.** O `order-service` publica dentro da requisição, depois do commit, e
  espera a confirmação do broker (`acks=all`) por até `PUBLISH_TIMEOUT` (padrão 5 s), com as
  retentativas do produtor em memória. Esgotado o tempo, o evento é dado como perdido e a API responde
  **sucesso** mesmo assim, porque a mudança já está commitada (`PUBLISH_FAILURE_RESPONSE=error` troca
  por 503). Consequência medida no ensaio: durante a falha a latência da API sobe para o valor do
  timeout, e parte das publicações dadas como falhas ainda chega depois, pela retentativa.
- **Produtor do Kafka Connect no B.** `producer.delivery.timeout.ms` está no máximo, para a tarefa do
  Debezium esperar a fila voltar em vez de falhar. Com o padrão do Kafka (2 min), a tarefa vai para
  `FAILED` em janelas longas e exige reinício manual (`CDC_PRODUCER_DELIVERY_TIMEOUT_MS=120000` e
  `--reiniciar-conector-falho` reproduzem esse caso).
- **Snapshot no B.** O conector usa `snapshot.mode=no_data` e é registrado depois da carga inicial:
  só as mudanças feitas pela API são propagadas, como no A.
- **Consumidor.** Entrega pelo menos uma vez (offset gravado a cada 1 s, depois da gravação no
  destino). O pedido só é sobrescrito por versão maior, então duplicatas e atrasadas ficam
  registradas em `received_events` mas não regridem a projeção. `CONSUMER_SESSION_TIMEOUT` (10 s)
  pesa no tempo até a entrega ser retomada, igualmente nos dois modos.
- **Decomposição.** Por ora são dois serviços (origem e destino), o suficiente para os Cenários 1, 2, 3
  e 5. O Cenário 4 vai exigir pelo menos três serviços com compensação.
- **Réplica sem slot físico (Cenário 2).** A réplica faz streaming sem slot de replicação, então o
  único slot da origem continua sendo o do Debezium, que não existe na réplica (PostgreSQL 16 não
  sincroniza slots lógicos). O atraso de replicação é provocado tirando a réplica da rede; congelar o
  contêiner não serve, porque o kernel continua aceitando o WAL no buffer do socket.
- **Leitura em lotes (Cenário 5).** Desligada por padrão; só o Cenário 5 a liga. O lote é o que já
  está no buffer do leitor do kafka-go, até `CONSUMER_MAX_POLL_RECORDS`.

## Estrutura

```
cmd/order-service      serviço de negócio (origem)
cmd/shipping-service   consumidor (destino)
cmd/seed               carga inicial a partir dos CSVs
internal/              modelo, publicação no Kafka, métricas, configuração
db/origem, db/destino  esquemas SQL
db/replica             inicialização da réplica da origem (Cenário 2)
infra/kafka            criação dos tópicos
infra/debezium         configuração e registro do conector
scripts/carga          gerador de carga e testes de carga
scripts/cenario1       injeção de falha do Cenário 1
scripts/cenario2       promoção de réplica
scripts/cenario3       reinícios repetidos do componente de propagação
scripts/cenario5       queda de consumidor e rebalanceamento
scripts/comum/execucao.py  fases comuns dos cenários 2, 3 e 5
scripts/comum          dataset, controle do ambiente, coleta e análise
scripts/painel         painel web com o diagrama, as falhas e os scripts
scripts/dados          amostra sintética
```
