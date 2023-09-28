from apiTwitch import apiTrends, BigQuery
import pandas as pd
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

def run(request=None):
    df = apiTrends().trends(inicio=None,fim=25)
    BigQuery('googleTrends',df).toBigQuery('project','append')

    return 'fim'
