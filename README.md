📉 Risk Bank — Análise de Risco Bancário com IF.data (BCB)

Dashboard e pipeline de dados para análise de risco de instituições financeiras brasileiras, utilizando dados públicos do IF.data (Banco Central do Brasil).
O projeto realiza ingestão, normalização, cálculo de score de risco e disponibiliza os resultados em um dashboard interativo (Streamlit).

🚀 Visão Geral

O Risk Bank foi criado para responder à pergunta:

“Com base em dados públicos, quais instituições apresentam maior risco financeiro relativo?”

O projeto entrega:

🔄 Pipeline de dados automatizado (IF.data → Banco)

🧠 Normalização semântica de indicadores financeiros

📊 Score de risco explicável (0–100), com drivers

🖥️ Dashboard profissional para análise e comparação

☁️ Deploy-ready para GitHub + Streamlit Community Cloud

🧱 Arquitetura
IF.data (BCB)
   ↓
[ ingest_ifdata ]
   ↓
Tabela bruta (ifdata_indicators)
   ↓
[ normalize_ifdata ]
   ↓
MART financeiro (mart_bank_metrics)
   ↓
[ risk_score ]
   ↓
MART de risco (mart_bank_risk)
   ↓
Streamlit Dashboard

🛠️ Stack Tecnológica

Python 3.13

PostgreSQL

SQLAlchemy 2.x

psycopg 3

Pandas

Streamlit

Altair

HTTPX

Pydantic Settings

IF.data (Banco Central do Brasil)

📂 Estrutura do Projeto
risk_bank/
├── apps/
│   └── dashboard/
│       └── app.py              # Dashboard Streamlit
│
├── core/
│   ├── __init__.py
│   ├── db.py                   # Engine / Session
│   └── settings.py             # Configurações (env / secrets)
│
├── pipelines/
│   ├── ingest_ifdata.py        # Ingestão IF.data (OData)
│   ├── normalize_ifdata.py     # Normalização semântica
│   ├── risk_score.py           # Score de risco + drivers
│   └── audit_semantic_map.py   # Auditoria de mapeamento
│
├── .streamlit/
│   └── config.toml             # Tema do dashboard
│
├── requirements.txt
├── README.md
└── pyproject.toml

📊 Métricas Calculadas

O projeto normaliza e calcula, entre outras:

Ativo Total

Patrimônio Líquido

Lucro Líquido

Índice de Basileia (%)

Liquidez

Inadimplência (%)

ROA (%)

Alavancagem (Ativo / PL)

🧠 Score de Risco

Escala 0 → 100 (quanto maior, pior)

Classificação:

🟢 BAIXO

🟡 MÉDIO

🔴 ALTO

Cada score possui drivers explicáveis, ex.:

{
  "basileia": {"value": 9.2, "score": 20},
  "liquidez": {"value": 0.95, "score": 18},
  "roa": {"value": -0.4, "score": 12}
}

▶️ Executando Localmente
1️⃣ Instalar dependências
poetry install
# ou
pip install -r requirements.txt

2️⃣ Configurar variáveis de ambiente
DATABASE_URL=postgresql+psycopg://user:senha@host:5432/riskdb
IFDATA_ODATA_BASE=https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata
REQUEST_TIMEOUT_S=30

3️⃣ Rodar pipeline de dados
# Ingestão
poetry run python -m pipelines.ingest_ifdata

# Normalização
poetry run python -m pipelines.normalize_ifdata --ref-date 2025-09-30

# Cálculo de risco
poetry run python -m pipelines.risk_score --ref-date 2025-09-30

4️⃣ Rodar o dashboard
poetry run streamlit run apps/dashboard/app.py


Acesse:
👉 http://localhost:8501

☁️ Deploy (Streamlit Community Cloud)

Suba o repositório no GitHub

Crie um app no Streamlit Cloud

Configure os Secrets:

DATABASE_URL="postgresql+psycopg://user:senha@host:5432/riskdb"
IFDATA_ODATA_BASE="https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata"
REQUEST_TIMEOUT_S="30"


Entry point:

apps/dashboard/app.py

🔄 Automação do Pipeline

O dashboard somente consome dados.
O pipeline pode ser executado via:

💻 Máquina local

⚙️ GitHub Actions (cron)

🖥️ Servidor / VPS

O banco deve ser PostgreSQL gerenciado (Neon, Supabase, Railway, etc.)

⚠️ Aviso Legal

Este projeto tem finalidade educacional e analítica.
O score apresentado não é recomendação de investimento nem substitui análises regulatórias oficiais.

👤 Autor

Pedro Santos (psholiveira)
🔗 GitHub: https://github.com/psholiveira

Projeto desenvolvido com foco em:

Engenharia de Dados

Análise de Risco

Data Products

Deploy em Cloud

📌 Próximos Passos (Roadmap)

 Comparação entre datas-base

 Histórico temporal de risco

 Alertas automáticos

 API pública de consulta

 Autenticação no dashboard