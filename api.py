

# import urllib

import requests

from urllib import parse


url_yahoo = "http://query.yahooapis.com/v1/public/yql?"
dados = {
    "q": "select * from yahoo.finance.historicaldata where symbol in \
 ('ABEV3.SA') and startDate = '2017-03-01' and endDate = '2017-04-06'",
    "format": "json",
    "diagnostics": "true",
    "env": "store://datatables.org/alltableswithkeys"
}
r = parse.urlencode(dados)

print(url_yahoo + r)


"""


ABEV3

http://finance.yahoo.com/d/quotes.csv?s=ABEV3.SA&f=vpt1


precho fechamento
valume
data


tem q por .SA no fim do codigo da acao

dados de 2012 ate hoje


doc
http://wern-ancheta.com/blog/2015/04/05/getting-started-with-the-yahoo-finance-api/

--
anbev
select * from yahoo.finance.historicaldata where symbol in ('ABEV3.SA') and startDate = '2017-03-01' and endDate = '2017-04-06'
yahoo
select * from yahoo.finance.historicaldata where symbol in ('YHOO') and startDate = '2009-09-11' and endDate = '2010-03-10'

http://query.yahooapis.com/v1/public/yql?q=select%20%2a%20from%20yahoo.finance.historicaldata%20where%20symbol%20in%20%28%27YHOO%27%29%20and%20startDate%20=%20%272009-09-11%27%20and%20endDate%20=%20%272010-03-10%27&diagnostics=true&env=store://datatables.org/alltableswithkeys

"""
