import pandas as pd
import datetime
import pandas_gbq
from pytrends.request import TrendReq
import numpy as np
from google.oauth2 import service_account
from time import sleep
import gspread
from oauth2client.service_account import ServiceAccountCredentials

class apiTrends():
  def autenticar_gspread(self):
      scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
      credentials = ServiceAccountCredentials.from_json_keyfile_name("key.json", scope)
      
      # Crie um cliente gspread
      client = gspread.authorize(credentials)
      return client

  def dadosContatos(self):
      client = self.autenticar_gspread()
      sh = client.open_by_url('url_da_planilha')
      ws = sh.worksheet('Página1')
      return pd.DataFrame(ws.get_all_records())

  def trends(self, inicio=None, fim=None):
      lista = []
      cont = 0
      trends = TrendReq(hl='pt-br', tz=360)
      Trends_Nome = list(self.dadosContatos()['Trends_Nome'])[inicio:fim]
      nome = list(self.dadosContatos()['Nome'])[inicio:fim]
      for i in range(len(nome)):
        try:
          sleep(1)
          cont +=1
          print(cont)
          print(Trends_Nome[i])
          trends.build_payload(kw_list=[Trends_Nome[i]],geo='BR',cat=0,timeframe="today 1-m")
          lista.extend([np.concatenate((trends.interest_over_time().dropna().reset_index().values[x],[Trends_Nome[i],nome[i]])) for x in range(len(nome))])
        except:
          pass
      df = pd.DataFrame(lista).rename(columns={0:'Data',1:'Valor',2:'Partial',3:'Nome'})
      return df


class BigQuery():
  def __init__(self,nomeTabela,dataFrame=False):
      self.nomeTabela = nomeTabela
      self.dataFrame  = dataFrame
  
  def toBigQuery(self, local,regra):
      return self.dataFrame.to_gbq(destination_table=local + '.' + self.nomeTabela,
                                    project_id='project_name', if_exists=regra))
