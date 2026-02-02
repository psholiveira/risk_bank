from sqlalchemy import text
from core.db import engine


SEED_SQL = """
INSERT INTO ifdata_indicators (ref_date, institution_id, institution_name, indicator, value)
VALUES
  ('2024-12-31','TESTE001','Banco Digital Teste','Basileia',12.5),
  ('2024-12-31','TESTE001','Banco Digital Teste','Liquidez',1.35),
  ('2024-12-31','TESTE001','Banco Digital Teste','ROA',0.45),
  ('2024-12-31','TESTE001','Banco Digital Teste','Inadimplencia',3.2),

  ('2024-12-31','TESTE002','Banco Digital Exemplo','Basileia',9.8),
  ('2024-12-31','TESTE002','Banco Digital Exemplo','Liquidez',0.95),
  ('2024-12-31','TESTE002','Banco Digital Exemplo','ROA',-0.10),
  ('2024-12-31','TESTE002','Banco Digital Exemplo','Inadimplencia',6.5)
ON CONFLICT (ref_date, institution_id, indicator) DO UPDATE
SET
  value = EXCLUDED.value,
  institution_name = EXCLUDED.institution_name;
"""


def run():
    with engine.begin() as conn:
        conn.execute(text(SEED_SQL))
    print("OK: seed inserido/atualizado.")


if __name__ == "__main__":
    run()
