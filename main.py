"""

dados entre
2016-01-01 = hoje

vou baixar os arquivo depois vou fz o dowload
"""

import datetime
import json
from urllib import parse

import requests

import utils



######################################################################
url_yahoo = "http://query.yahooapis.com/v1/public/yql?"
data_inicio = '2016-01-01'
data_fim = None
diretorio = "arquivos"


def inserir_empresa():
    """
    Preenche a tabela de empresa, apartir do arquivo codigos.json
    """
    codigos = utils.get_config("codigos")["codigos"]
    conn = utils.get_db_conn()

    try:
        with conn.cursor() as cursor:
            sql = "INSERT INTO `empresa` VALUES (NULL, %s)"
            for codigo in codigos:
                cursor.execute(sql, (codigo))
        conn.commit()

    finally:
        conn.close()


def biginsert(lst_to_insert, conn):
    passo = 700
    sql_topo = "INSERT INTO `valor` (`id`, `owner`, `date`, `open`, `high`, `low`, `close`, `volume`) VALUES "
    contador = 0

    with conn.cursor() as cursor:
        sub_list = []
        for e in lst_to_insert:
            sub_list.append(e)
            contador += 1

            if contador == passo:  # chegou ao maximo
                sql = sql_topo + ", \n".join(sub_list)
                cursor.execute(sql)
                # print(sql + "\n")
                sub_list.clear()  # limpa a lista para inserir mais dados
                contador = 0

        # insera a sobra da lista antes de passo
        if len(sub_list) > 0:
            sql = sql_topo + ", \n".join(sub_list)
            cursor.execute(sql)
            # print(sql + "\n")5147

    conn.commit()

    return 0


def tratar_lista(lst, id_):
    # return [[id_] + e.keys()[1:-1] for e in lst]
    lst_saida = []
    for e in lst:
        # ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        tmp = [id_, e['Date'], float(e['Open']), float(e['High']),
               float(e['Low']), float(e['Close']), int(e['Volume'])]
        lst_saida.append(tmp)

    return lst_saida


#['Low', 'Date', 'Adj_Close', 'Volume', 'Open', 'Close', 'Symbol', 'High']
"""
  `id` BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL,
  `owner` BIGINT NOT NULL,
  `date` DATE NOT NULL,
  `open` FLOAT NOT NULL,
  `high` FLOAT NOT NULL,
  `low` FLOAT NOT NULL,
  `close` FLOAT NOT NULL,
  `volume` BIGINT NOT NULL,
"""
def salvar():
    """
    glob para listar os arquivos, pega e salva naquele esquema de 700
    """
    # lst = glob.glob(diretorio + "/*")
    # conn = utils.get_db_conn()
    # for arquivo in lst:
    sql = "SELECT * FROM `empresa`"
    lst_empresa = geraln_query(sql)
    conn = utils.get_db_conn()
    lst_valores = []
    for id_, codigo in lst_empresa:
        with open(diretorio + "/" + codigo + ".json") as f:
            conteudo = json.load(f)
            conteudo["query"]["results"]["quote"]






def geraln_query(sql):
    conn = utils.get_db_conn()
    lst = []
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            lst = cursor.fetchall()
        conn.commit()

    finally:
        conn.close()

    return lst


def get_codigos():
    sql = "SELECT `cod` from `empresa`"
    return geraln_query(sql)


def baixar():
    # essa lista do codigo vai vir do banco
    lst_cogigo = get_codigos()
    query = "select * from yahoo.finance.historicaldata where symbol in \
('{0}') and startDate = '{1}' and endDate = '{2}'"

    url_query = {
            "q": None,
            "format": "json",
            "diagnostics": "true",
            "env": "store://datatables.org/alltableswithkeys"
        }
    data_inicio = datetime.datetime.now().strftime(utils.formato_us)

    for codigo in lst_cogigo:
        codigo_formatodo = codigo + ".SA"

        with open(diretorio + "/" + codigo + ".json", mode='wb') as f:
            url_query["q"] = query.format(codigo_formatodo, data_inicio,
                                          data_fim)

            url_get = url_yahoo + parse.urlencode(url_query)
            resposta = requests.get(url_get, stream=True)

            if resposta.ok():
                for bloco in resposta.iter_content(1024):
                    f.write(bloco)


if __name__ == '__main__':
    c = {
          "Symbol": "ABEV3.SA",
          "Date": "2017-04-05",
          "Open": "18.08",
          "High": "18.18",
          "Low": "17.81",
          "Close": "17.88",
          "Volume": "8734100",
          "Adj_Close": "17.88"
        }

    print(repr(tratar_lista([c], 1)))
