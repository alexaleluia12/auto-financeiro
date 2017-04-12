"""

dados entre
2016-01-01 = hoje

vou baixar os arquivo depois vou fz o dowload
"""

import datetime
import json
from urllib import parse
import os
import csv

import requests

import utils

perdidos = """ALUP11.SA
BBSD11.SA
BOVA11.SA
BRAX11.SA
DIVO11.SA
ECOO11.SA
FIND11.SA
GOVE11.SA
ISUS11.SA
KLBN11.SA
MATB11.SA
PIBB11.SA
SANB11.SA
SMAL11.SA
TAEE11.SA
TIET11.SA
VVAR11.SA
XBOV11.SA"""


######################################################################
url_yahoo = "http://query.yahooapis.com/v1/public/yql?"
data_inicio = '2015-01-01'
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
    template = "({0}, {1}, '{2}', {3}, {4}, {5}, {6}, {7})"
    with conn.cursor() as cursor:
        sub_list = []
        for e in lst_to_insert:
            # print(e)
            sub_list.append(template.format(*e))
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
            # print(sql + "\n")

    conn.commit()

    return 0


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
 Date,Open,High,Low,Close,Volume
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
    lst_nao_salvaos = []
    lst_invertidos = None
    primeiro = 0
    try:
        conn = utils.get_db_conn()
        for id_, codigo in lst_empresa:
            with open(diretorio + "/" + codigo + ".json") as f:
                conteudo = csv.reader(f)
                primeiro = 0
                lst_invertidos = []
                for e in conteudo:
                    if primeiro == 0:
                        primeiro = 1
                        continue
                    lst_invertidos.append(["NULL", id_] + e[:-1])

                # lst_invertidos contem a linhas em ordem inversa
                biginsert(lst_invertidos[::-1], conn)
                print("Salvou", codigo)

    finally:
        conn.close()










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


#http://ichart.finance.yahoo.com/table.csv?a=0&b=1&e=11&g=d&c=2015&d=3&f=2017&s=BRKM5.SA
#ja fiz o dowload, nao preciso mais
def baixar():
    # essa lista do codigo vai vir do banco
    # lst_codigo = get_codigos()
    lst_codigo = perdidos.split('\n')
    template = "http://ichart.finance.yahoo.com/table.csv?a=0&b=1&e=11&g=d&c=2015&d=3&f=2017&s={0}"
    lst_falha = []
    for codigo in lst_codigo:
        # codigo_formatodo = codigo[0] + ".SA"
        codigo_formatodo = codigo

        with open(diretorio + "/" + codigo[0] + ".json", mode='wb') as f:

            url_get = template.format(codigo_formatodo)
            resposta = requests.get(url_get, stream=True)

            if resposta.ok:
                for bloco in resposta.iter_content(1024):
                    f.write(bloco)
                print("baixou ", codigo_formatodo)
            else:
                print("nao foi possivel baixar", codigo_formatodo)
                lst_falha.append(codigo_formatodo)

    print(lst_falha)


# atualizar no banco tmb
# esse codigos eu nao vou trabalhar
# nao precisa mais
def remover():
    lst = ['ALUP11.SA', 'BBSD11.SA', 'BOVA11.SA', 'BRAX11.SA', 'DIVO11.SA', 'ECOO11.SA', 'FIND11.SA', 'GOVE11.SA', 'ISUS11.SA', 'KLBN11.SA', 'MATB11.SA', 'PIBB11.SA', 'SANB11.SA', 'SMAL11.SA', 'TAEE11.SA', 'TIET11.SA', 'VVAR11.SA', 'XBOV11.SA']
    template_rm = "rm " + diretorio + "/{0}.json"
    limpos = []
    for e in lst:
        # os.system(template_rm.format(e[:-3]))
        # print("$", template_rm.format(e[:-3]))
        limpos.append(e[:-3])
    conn = utils.get_db_conn()
    try:
        with conn.cursor() as cursor:
            sql = "delete from `empresa` WHERE cod IN ( '" + "', '".join(limpos) + "')"
            print(sql)
            cursor.execute(sql)
        conn.commit()

    finally:
        conn.close()




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

    # print(repr(tratar_lista([c], 1)))
    salvar()
    # a = get_codigos()
    # for i in a:
    #     print(i[0] + ".SA")
