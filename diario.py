"""
faz o download diario dos dados
"""
import datetime

import requests

import main
import utils

def atualizar():
    """
    formato:  Open,High,Low,Close,Volume
    Data eh de hoje
    ol2l3pv
    referencia: http://wern-ancheta.com/blog/2015/04/05/getting-started-with-the-yahoo-finance-api/

    vo chamar tudo de uma vez:
    http://finance.yahoo.com/d/quotes.csv?s=GOOGL,AAPL,MSFT,FB&f=abo
    traz na mesma sequencia passada

    http://finance.yahoo.com/d/quotes.csv?s=ABEV3.SA&f=ol2l3pv
    High e Low pode nao ter
    """
    conn = utils.get_db_conn()
    agora = datetime.datetime.now().strftime(utils.formato_us)
    lst_insert = []
    lst_codigo = main.geraln_query('SELECT id, cod FROM `empresa`')
    codigos = [i[1]+".SA" for i in lst_codigo]
    mapas = {i[1]+".SA": i[0] for i in lst_codigo}  # como eu excluir alguns as id's podem nao estar em sequencia
    url = "http://finance.yahoo.com/d/quotes.csv?s=" + ",".join(codigos) + "&f=ol2l3pv"

    try:
        r = requests.get(url)
        if r.ok:
            conteudo = r.text.split("\n")
            for index, elemento in enumerate(conteudo):
                tmplst = elemento.split(',')
                if len(tmplst) != 5:
                    continue
                try:
                    _open = float(tmplst[0])
                except ValueError as e:
                    _open = 0.0

                try:
                    _high = float(tmplst[1])
                except ValueError as e:
                    _high = 0.0

                try:
                    _low = float(tmplst[2])
                except ValueError as e:
                    _low = 0.0

                try:
                    _close = float(tmplst[3])
                except ValueError as e:
                    print("Exesse valor deveria existir(", codigos[index], ")")
                    raise Exception("Valor de fechamento eh crucial")
                    _close = 0.0

                try:
                    _volume = int(tmplst[4])
                except ValueError as e:
                    _volume = 0.0

                dono = mapas[codigos[index]]
                lst_insert.append(["NULL", dono, agora, _open, _high, _low, _close, _volume])

            if len(lst_insert) > 0:

                main.biginsert(lst_insert, conn)
    finally:
        conn.close()


if __name__ == '__main__':
    atualizar()
