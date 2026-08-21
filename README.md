<div align="center">

# 📉 Risk Bank

**Pipeline de dados e dashboard analítico para avaliação de risco de instituições financeiras brasileiras, a partir dos dados públicos do IF.data (Banco Central do Brasil).**

[![Python](https://img.shields.io/badge/Python-3.13+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.x-D71F00?logo=sqlalchemy&logoColor=white)](https://www.sqlalchemy.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Poetry](https://img.shields.io/badge/Poetry-60A5FA?logo=poetry&logoColor=white)](https://python-poetry.org/)
[![Ruff](https://img.shields.io/badge/lint-ruff-D7FF64?logo=ruff&logoColor=black)](https://docs.astral.sh/ruff/)
[![mypy](https://img.shields.io/badge/types-mypy_strict-2A6DB2)](https://mypy-lang.org/)

</div>

---

## 📑 Sumário

- [Sobre o projeto](#-sobre-o-projeto)
- [Arquitetura de dados](#-arquitetura-de-dados)
- [Stack](#-stack)
- [Estrutura do projeto](#-estrutura-do-projeto)
- [Métricas calculadas](#-métricas-calculadas)
- [Score de risco](#-score-de-risco)
- [Executando localmente](#-executando-localmente)
- [Variáveis de ambiente](#-variáveis-de-ambiente)
- [Qualidade de código](#-qualidade-de-código)
- [Deploy](#-deploy)
- [Automação do pipeline](#-automação-do-pipeline)
- [Roadmap](#-roadmap)
- [Aviso legal](#-aviso-legal)
- [Autor](#-autor)

---

## 📖 Sobre o projeto

O **Risk Bank** existe para responder a uma pergunta concreta:

> *Com base exclusivamente em dados públicos, quais instituições financeiras brasileiras apresentam maior risco relativo?*

O Banco Central publica trimestralmente, via IF.data, um conjunto extenso de indicadores contábeis e prudenciais de todas as instituições supervisionadas. Os dados existem, mas chegam em formato bruto, com nomenclatura inconsistente entre períodos e sem qualquer camada analítica. Este projeto transforma esse insumo em um produto de dados utilizável.

**O que o projeto entrega:**

| | Entrega |
|:--|:--|
| 🔄 | Pipeline automatizado de ingestão do IF.data via API OData |
| 🧠 | Normalização semântica dos indicadores, resolvendo divergências de nomenclatura |
| 📊 | Score de risco explicável de 0 a 100, com os drivers que o compõem |
| 🖥️ | Dashboard interativo para análise, filtro e comparação entre instituições |
| ☁️ | Estrutura pronta para deploy no Streamlit Community Cloud |

O princípio central é a **explicabilidade**: o score nunca é um número isolado. Cada avaliação carrega os indicadores que a produziram e o peso de cada um, de modo que a conclusão seja auditável em vez de aceita por confiança.

---

## 🧱 Arquitetura de dados

O projeto segue uma arquitetura em camadas, no estilo *medallion*, separando dado bruto de dado modelado:

```
        IF.data (Banco Central do Brasil)
                     │  API OData
                     ▼
        ┌────────────────────────────┐
        │  pipelines/ingest_ifdata   │   Camada de ingestão
        └────────────┬───────────────┘
                     ▼
            ifdata_indicators            ← Tabela bruta (raw)
                     │
        ┌────────────▼───────────────┐
        │ pipelines/normalize_ifdata │   Normalização semântica
        └────────────┬───────────────┘
                     ▼
            mart_bank_metrics            ← MART financeiro
                     │
        ┌────────────▼───────────────┐
        │   pipelines/risk_score     │   Modelo de risco
        └────────────┬───────────────┘
                     ▼
             mart_bank_risk              ← MART de risco + drivers
                     │
                     ▼
        ┌────────────────────────────┐
        │  Streamlit Dashboard       │   Camada de consumo
        └────────────────────────────┘
```

**Separação de responsabilidades:** o dashboard apenas *consome* os marts. Nenhum cálculo de risco acontece em tempo de renderização — isso mantém a interface rápida e garante que o número exibido seja o mesmo que qualquer outra ferramenta leria do banco.

---

## 🛠 Stack

### Aplicação

| Tecnologia | Papel |
|:--|:--|
| Python 3.13+ | Linguagem |
| PostgreSQL 16 | Armazenamento das camadas raw e mart |
| SQLAlchemy 2.x | ORM e camada de acesso a dados |
| psycopg 3 | Driver PostgreSQL |
| Pandas | Transformação e agregação |
| HTTPX | Cliente HTTP para a API OData do BCB |
| Tenacity | Política de retry na ingestão |
| Pydantic Settings | Configuração tipada via variáveis de ambiente |
| Streamlit | Dashboard interativo |
| Altair · Plotly | Visualizações |
| FastAPI · Uvicorn | Base para a API de consulta (em desenvolvimento) |

### Desenvolvimento

| Ferramenta | Papel |
|:--|:--|
| Poetry | Gerenciamento de dependências e ambiente |
| Ruff | Linter e formatador (`line-length = 100`) |
| mypy | Verificação de tipos em modo **strict** |
| pytest · pytest-asyncio | Testes |
| Bandit | Análise estática de segurança |
| pre-commit | Automação dos checks antes do commit |
| Docker Compose | PostgreSQL local |

---

## 📂 Estrutura do projeto

```
risk_bank/
├── apps/
│   └── dashboard/
│       └── app.py                  # Dashboard Streamlit
│
├── core/
│   ├── __init__.py
│   ├── db.py                       # Engine e sessão SQLAlchemy
│   └── settings.py                 # Configuração via Pydantic Settings
│
├── pipelines/
│   ├── ingest_ifdata.py            # Ingestão IF.data (OData)
│   ├── normalize_ifdata.py         # Normalização semântica
│   ├── risk_score.py               # Cálculo de score e drivers
│   └── audit_semantic_map.py       # Auditoria do mapeamento semântico
│
├── docker-compose.yml              # PostgreSQL 16 local
├── pyproject.toml                  # Dependências e configuração de ferramentas
└── poetry.lock
```

> O script `audit_semantic_map.py` é o que torna a normalização confiável: ele verifica se todos os indicadores esperados foram mapeados corretamente, evitando que uma mudança de nomenclatura no IF.data passe despercebida e corrompa os marts silenciosamente.

---

## 📊 Métricas calculadas

A camada de normalização consolida, entre outros indicadores:

| Métrica | Descrição |
|:--|:--|
| **Ativo Total** | Tamanho do balanço da instituição |
| **Patrimônio Líquido** | Capital próprio |
| **Lucro Líquido** | Resultado do período |
| **Índice de Basileia (%)** | Adequação de capital frente aos ativos ponderados pelo risco |
| **Liquidez** | Capacidade de honrar obrigações de curto prazo |
| **Inadimplência (%)** | Proporção da carteira em atraso |
| **ROA (%)** | Retorno sobre ativos |
| **Alavancagem** | Razão entre Ativo Total e Patrimônio Líquido |

---

## 🧠 Score de risco

O score varia de **0 a 100**, onde **valores maiores indicam maior risco relativo**:

| Faixa | Classificação |
|:--:|:--|
| 🟢 | **BAIXO** |
| 🟡 | **MÉDIO** |
| 🔴 | **ALTO** |

Cada score é acompanhado dos **drivers** que o compuseram, permitindo entender exatamente de onde veio a nota:

```json
{
  "basileia": { "value": 9.2,  "score": 20 },
  "liquidez": { "value": 0.95, "score": 18 },
  "roa":      { "value": -0.4, "score": 12 }
}
```

No exemplo acima, a instituição é penalizada por um índice de Basileia próximo do mínimo regulatório, liquidez abaixo de 1 e ROA negativo — cada fator com sua contribuição isolada e verificável.

> ⚠️ O score é **relativo** e comparativo entre instituições da mesma data-base. Ele não replica nem substitui as metodologias de rating regulatório ou de agências de classificação.

---

## ▶️ Executando localmente

### Pré-requisitos

- Python 3.13+
- [Poetry](https://python-poetry.org/)
- Docker e Docker Compose (para o banco local)

### 1. Instalar dependências

```bash
poetry install
```

### 2. Subir o banco de dados

O repositório inclui um `docker-compose.yml` com PostgreSQL 16 pronto para uso:

```bash
docker compose up -d
```

Isso cria o banco `riskdb` exposto na porta **5433** do host (para não conflitar com uma instalação local de PostgreSQL na 5432).

### 3. Configurar as variáveis de ambiente

Crie um arquivo `.env` na raiz:

```env
DATABASE_URL=postgresql+psycopg://risk:risk@localhost:5433/riskdb
IFDATA_ODATA_BASE=https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata
REQUEST_TIMEOUT_S=30
```

### 4. Executar o pipeline

Os três estágios devem rodar em ordem — cada um consome a saída do anterior:

```bash
# 1. Ingestão dos dados brutos do IF.data
poetry run python -m pipelines.ingest_ifdata

# 2. Normalização semântica → mart financeiro
poetry run python -m pipelines.normalize_ifdata --ref-date 2025-09-30

# 3. Cálculo do score de risco → mart de risco
poetry run python -m pipelines.risk_score --ref-date 2025-09-30
```

> O parâmetro `--ref-date` deve corresponder a uma data-base trimestral publicada pelo BCB (31/03, 30/06, 30/09 ou 31/12).

### 5. Abrir o dashboard

```bash
poetry run streamlit run apps/dashboard/app.py
```

Acesse **http://localhost:8501**

---

## 🔑 Variáveis de ambiente

| Variável | Obrigatória | Padrão | Descrição |
|:--|:--:|:--|:--|
| `DATABASE_URL` | ✅ | — | String de conexão SQLAlchemy com o PostgreSQL. |
| `IFDATA_ODATA_BASE` | ✅ | — | URL base do serviço OData do IF.data. |
| `REQUEST_TIMEOUT_S` | ➖ | `30` | Timeout, em segundos, das requisições ao BCB. |

As configurações são carregadas por **Pydantic Settings** em `core/settings.py`, o que significa validação de tipos no boot: uma variável ausente ou malformada falha imediatamente, em vez de gerar erro obscuro no meio do pipeline.

---

## ✅ Qualidade de código

O projeto já vem com o ferramental configurado no `pyproject.toml`:

```bash
poetry run ruff check .          # Lint
poetry run ruff format .         # Formatação
poetry run mypy .                # Tipagem (modo strict)
poetry run bandit -r core pipelines apps   # Análise de segurança
poetry run pytest                # Testes
```

Para rodar tudo automaticamente antes de cada commit:

```bash
poetry run pre-commit install
```

**Configuração adotada:** `ruff` com `line-length = 100` e `mypy` em modo **strict**, com `warn_unused_configs` ativo.

---

## ☁️ Deploy

### Streamlit Community Cloud

1. Publique o repositório no GitHub.
2. Crie um app no [Streamlit Cloud](https://share.streamlit.io/).
3. Defina o entry point: `apps/dashboard/app.py`.
4. Configure os **Secrets**:

```toml
DATABASE_URL = "postgresql+psycopg://user:senha@host:5432/riskdb"
IFDATA_ODATA_BASE = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata"
REQUEST_TIMEOUT_S = "30"
```

O banco deve ser um **PostgreSQL gerenciado** acessível pela internet — Neon, Supabase, Railway ou equivalente. O container do `docker-compose.yml` serve apenas ao desenvolvimento local.

---

## 🔄 Automação do pipeline

O dashboard é somente leitura. A atualização dos dados depende da execução periódica do pipeline, que pode ser agendada em:

| Ambiente | Observação |
|:--|:--|
| 💻 Máquina local | Execução manual, adequado para exploração |
| ⚙️ GitHub Actions | Workflow com `schedule` (cron) — a via recomendada |
| 🖥️ VPS / servidor | Cron do sistema operacional |

Como o IF.data é publicado trimestralmente, uma execução mensal já é suficiente para capturar cada nova data-base pouco depois da divulgação.

---

## 🧭 Roadmap

- [x] Ingestão automatizada via OData
- [x] Normalização semântica com auditoria de mapeamento
- [x] Score de risco explicável com drivers
- [x] Dashboard interativo em Streamlit
- [ ] 📅 Comparação entre datas-base
- [ ] 📈 Histórico temporal da evolução do risco
- [ ] 🔔 Alertas automáticos para deterioração de indicadores
- [ ] 🌐 API pública de consulta (FastAPI já disponível nas dependências)
- [ ] 🔐 Autenticação no dashboard
- [ ] ⚙️ Workflow de GitHub Actions para execução agendada
- [ ] 🧪 Cobertura de testes dos pipelines

---

## ⚠️ Aviso legal

Este projeto tem finalidade **educacional e analítica**. O score apresentado é uma medida comparativa construída a partir de dados públicos e **não constitui recomendação de investimento**, análise de crédito ou substituto de avaliações regulatórias oficiais. Os dados originais são de responsabilidade do Banco Central do Brasil; eventuais erros de interpretação ou transformação são de responsabilidade deste projeto, não da fonte.

**Fonte dos dados:** [IF.data — Banco Central do Brasil](https://www3.bcb.gov.br/ifdata/)

---

## 👤 Autor

**Pedro Santos Oliveira**

- GitHub: [@psholiveira](https://github.com/psholiveira)

Projeto desenvolvido com foco em **Engenharia de Dados**, **Análise de Risco**, **Data Products** e **deploy em cloud**.
