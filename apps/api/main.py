from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from core.db import get_db
from core.models import IfdataIndicator
from core.risk import RiskInputs, score_risco
from core.models import MartBankMetrics


app = FastAPI(title="Análise de Risco - Bancos Digitais BR", version="0.1.0")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/banks")
def list_banks(db: Session = Depends(get_db)):
    rows = db.query(IfdataIndicator.institution_id, IfdataIndicator.institution_name).distinct().all()
    return [{"id": r[0], "name": r[1]} for r in rows]


@app.get("/metrics/{bank_id}")
def metrics(bank_id: str, ref_date: str, db: Session = Depends(get_db)):
    rows = (
        db.query(IfdataIndicator)
        .filter(IfdataIndicator.institution_id == bank_id, IfdataIndicator.ref_date == ref_date)
        .all()
    )
    return {
        "bank_id": bank_id,
        "ref_date": ref_date,
        "metrics": {r.indicator: r.value for r in rows},
    }


@app.get("/risk/{bank_id}")
def risk(bank_id: str, ref_date: str, db: Session = Depends(get_db)):
    rows = (
        db.query(IfdataIndicator)
        .filter(IfdataIndicator.institution_id == bank_id, IfdataIndicator.ref_date == ref_date)
        .all()
    )
    m = {r.indicator.lower(): r.value for r in rows}

    inputs = RiskInputs(
        basileia=m.get("basileia"),
        liquidez=m.get("liquidez"),
        roa=m.get("roa"),
        inadimplencia=m.get("inadimplencia"),
    )
    score, details = score_risco(inputs)
    return {"bank_id": bank_id, "ref_date": ref_date, "score": score, "explain": details}

@app.get("/mart/{bank_id}")
def mart(bank_id: str, ref_date: str, db: Session = Depends(get_db)):
    row = (
        db.query(MartBankMetrics)
        .filter(MartBankMetrics.institution_id == bank_id, MartBankMetrics.ref_date == ref_date)
        .one_or_none()
    )
    if not row:
        return {"bank_id": bank_id, "ref_date": ref_date, "mart": {}}

    return {
        "bank_id": bank_id,
        "ref_date": ref_date,
        "mart": {
            "basileia": row.basileia,
            "liquidez": row.liquidez,
            "roa": row.roa,
            "inadimplencia": row.inadimplencia,
            "ativos_total": row.ativos_total,
            "patrimonio_liquido": row.patrimonio_liquido,
            "resultado_liquido": row.resultado_liquido,
            "carteira_credito": row.carteira_credito,
        },
    }


@app.get("/risk/{bank_id}")
def risk(bank_id: str, ref_date: str, db: Session = Depends(get_db)):
    row = (
        db.query(MartBankMetrics)
        .filter(MartBankMetrics.institution_id == bank_id, MartBankMetrics.ref_date == ref_date)
        .one_or_none()
    )
    if not row:
        # fallback: sem mart ainda
        return {"bank_id": bank_id, "ref_date": ref_date, "score": 0, "explain": {"note": 1}}

    inputs = RiskInputs(
        basileia=row.basileia,
        liquidez=row.liquidez,
        roa=row.roa,
        inadimplencia=row.inadimplencia,
    )
    score, details = score_risco(inputs)
    return {"bank_id": bank_id, "ref_date": ref_date, "score": score, "explain": details}

