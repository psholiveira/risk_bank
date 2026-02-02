from sqlalchemy import String, Integer, Float, Date, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from core.db import Base


class IfdataIndicator(Base):
    __tablename__ = "ifdata_indicators"
    __table_args__ = (
        UniqueConstraint("ref_date", "institution_id", "indicator", name="uq_ifdata_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ref_date: Mapped[str] = mapped_column(String(10), index=True)  # "YYYY-MM-DD" (simples)
    institution_id: Mapped[str] = mapped_column(String(512), index=True)
    institution_name: Mapped[str] = mapped_column(String(255), index=True)

    indicator: Mapped[str] = mapped_column(String(80), index=True)  # ex: "Basileia", "Ativos"
    value: Mapped[float] = mapped_column(Float)

    from sqlalchemy import String, Integer, Float, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from core.db import Base


class MartBankMetrics(Base):
    __tablename__ = "mart_bank_metrics"
    __table_args__ = (
        UniqueConstraint("ref_date", "institution_id", name="uq_mart_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ref_date: Mapped[str] = mapped_column(String(10), index=True)  # YYYY-MM-DD
    institution_id: Mapped[str] = mapped_column(String(50), index=True)
    institution_name: Mapped[str] = mapped_column(String(255), index=True)

    # Indicadores canônicos (nulos se não encontrados)
    basileia: Mapped[float | None] = mapped_column(Float, nullable=True)
    liquidez: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Métricas de resultado / qualidade de ativos
    roa: Mapped[float | None] = mapped_column(Float, nullable=True)
    inadimplencia: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Para enriquecer análises futuras
    ativos_total: Mapped[float | None] = mapped_column(Float, nullable=True)
    patrimonio_liquido: Mapped[float | None] = mapped_column(Float, nullable=True)
    resultado_liquido: Mapped[float | None] = mapped_column(Float, nullable=True)
    carteira_credito: Mapped[float | None] = mapped_column(Float, nullable=True)

