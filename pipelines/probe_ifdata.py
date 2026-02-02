import httpx

BASE = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata"

def get(url, params):
    r = httpx.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    url = f"{BASE}/IfDataValores(AnoMes=@AnoMes,TipoInstituicao=@TipoInstituicao,Relatorio=@Relatorio)"
    rel = "1"

    # teste últimos 24 meses e 3 tipos
    anomes_list = [
        202512, 202509, 202506, 202503, 202412, 202409, 202406, 202403,
        202312, 202309, 202306, 202303, 202212
    ]
    tipos = [1, 2, 3, 4]

    for anomes in anomes_list:
        for t in tipos:
            params = {
                "$format": "json",
                "$top": 1,
                "@AnoMes": anomes,
                "@TipoInstituicao": t,
                "@Relatorio": f"'{rel}'",
            }
            data = get(url, params=params)
            n = len(data.get("value", []))
            if n > 0:
                print(f"OK: anomes={anomes} tipo={t} relatorio={rel} -> {n} linha(s)")
                return

    print("NÃO achei dados nos combos testados. Algo mudou no endpoint/params.")

if __name__ == "__main__":
    main()
