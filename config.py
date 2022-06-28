"""Here are the config files, directories or parameters that can change in all places"""

from datetime import date
from dateutil.relativedelta import relativedelta

######Date of begin and end of extracted data

initialdate = date(2019,1,1)
finaldate = date.today() + relativedelta(months=-2)
print('Data inicial de busca: ' + str(initialdate) + ' Ano-Mês-Dia')
print('Data final de busca: ' + str(finaldate))

######Database Configs
mysql = {
    "host": "localhost",
    "user": "root",
    "passwd": "my secret password",
    "db": "write-math",
}