import json
import pandas as pd
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus

key = open('kma_api_pkg/key_백관구.txt').read()
url = 'http://apis.data.go.kr/1360000/VilageFcstInfoService/getVilageFcst'

class load:
    def __init__(self, base_date, base_time, nx, ny, key = key, url = url):
        self.base_date = base_date
        self.base_time = base_time
        self.nx        = nx
        self.ny        = ny
        self.key       = key
        self.url       = url
    
    def _get_json(self):
        query = f"?{quote_plus('ServiceKey')}={self.key}&" +\
         urlencode({quote_plus('pageNo')    : '1',\
					quote_plus('numOfRows') : '300',\
					quote_plus('dataType')  : 'JSON',\
					quote_plus('base_date') : str(self.base_date),\
					quote_plus('base_time') : str(self.base_time),\
					quote_plus('nx')        : str(self.nx),\
					quote_plus('ny')        : str(self.ny)})
        request = Request(self.url + query)
        request.get_method = lambda: 'GET'
        content = urlopen(request).read().decode('utf-8')
        return content

    def _to_df(self, json_content):
        json_content = json.loads(json_content)['response']
        header = json_content['header']
        if header['resultMsg'] == 'NORMAL_SERVICE': pass
        else: raise InterruptedError(f"resultCode: {header['resultCode']}, resultMsg: {header['resultMsg']}")
        df_content = pd.DataFrame(json_content['body']['items']['item'])
        df_content['DT_base'] = df_content.apply(lambda x: x['baseDate'] + x['baseTime'], axis = 1)
        df_content['DT_fcst'] = df_content.apply(lambda x: x['fcstDate'] + x['fcstTime'], axis = 1)
        df = df_content.pivot(index = 'DT_fcst', columns = 'category', values = 'fcstValue')
        df['DT_base']    = self.base_date + self.base_time
        df['nx'.upper()] = self.nx
        df['ny'.upper()] = self.ny
        df.reset_index(inplace = True)
        df.sort_index(axis = 'columns', inplace = True)
        df.columns.name = None
        return df
    
    def run(self):
        return self._to_df(self._get_json())