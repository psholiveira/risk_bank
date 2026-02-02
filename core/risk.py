from dataclasses import dataclass


@dataclass(frozen=True)
class RiskInputs:
    basileia: float | None
    liquidez: float | None
    roa: float | None
    inadimplencia: float | None


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def score_risco(inputs: RiskInputs) -> tuple[int, dict]:
    score = 0.0
    details: dict[str, float] = {}

    # Basileia (%): menor = pior
    if inputs.basileia is None:
        score += 8
        details["basileia_penalty"] = 8
    else:
        bas = inputs.basileia
        if bas < 9:
            p = 35
        elif bas < 11:
            p = 20
        elif bas < 13:
            p = 10
        else:
            p = 0
        score += p
        details["basileia_penalty"] = p

    # Liquidez (índice): menor = pior
    if inputs.liquidez is None:
        score += 6
        details["liquidez_penalty"] = 6
    else:
        liq = inputs.liquidez
        if liq < 1.0:
            p = 25
        elif liq < 1.2:
            p = 15
        elif liq < 1.5:
            p = 8
        else:
            p = 0
        score += p
        details["liquidez_penalty"] = p

    # ROA (%): negativo = pior
    if inputs.roa is None:
        score += 4
        details["roa_penalty"] = 4
    else:
        roa = inputs.roa
        if roa < 0:
            p = 15
        elif roa < 0.3:
            p = 8
        else:
            p = 0
        score += p
        details["roa_penalty"] = p

    # Inadimplência (%): maior = pior
    if inputs.inadimplencia is None:
        score += 4
        details["inadimplencia_penalty"] = 4
    else:
        npl = inputs.inadimplencia
        if npl > 6:
            p = 15
        elif npl > 4:
            p = 10
        elif npl > 3:
            p = 6
        else:
            p = 0
        score += p
        details["inadimplencia_penalty"] = p

    score = clamp(score, 0, 100)
    return int(round(score)), details
