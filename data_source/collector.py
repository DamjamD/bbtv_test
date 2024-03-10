import requests
import pandas as pd
import datetime
import json
from io import BytesIO
from typing import List
import uuid

class APICollector:
    def __init__(self, azure, data, path):
      
        self._azure = azure
        self._buffer = None
        self._data = data
        self._path = path
        return

    def start(self, data, path):
        try:
        
            response = data    
            response = self.transformDf(response)        
            #response = self.convertToCsv(response)
       
            if self._buffer is not None:
                file_name = self.fileName(path)
                try:
                    self._azure.upload_file(self._buffer.getvalue(),file_name)
                    return True
                except Exception as e:
                    print(f"Erro ao Fazer Upload de Arquivo: {e}")
                    return False
        except Exception as error:
            print(f"Erro geral: {error}")
            return False
        return False
    

    def transformDf(self, df):
    
        try:
            df.columns = df.columns.str.replace(' ', '_')
            df.columns = df.columns.str.lower()

           
            try:
                numeric_columns = df.select_dtypes(include='number').columns
                df_numeric = df[numeric_columns]
                
                correlation_matrix = df_numeric.corr()        
                revenue_correlations = correlation_matrix['estimated_revenue_(usd)'].sort_values(ascending=False)
                revenue_correlations = revenue_correlations.drop('estimated_revenue_(usd)')
              
                revenue_correlations.to_csv('files\correlation.csv')
                

            except Exception as e:
                print(f"Error correlations: {e}")
                return None

            # Exibindo as correlações mais fortes
            print("Strongest correlations with revenue:")
            print(revenue_correlations)

     
            df['video_publish_time'] = pd.to_datetime(df['video_publish_time'])
            df['video_publish_time'] = df['video_publish_time'].dt.strftime('%Y-%m-%d')
         
            
            print(df.columns)
       
      
            return df
         
        except Exception as e:
            print(f"Error Tranforming DF: {e}")
            return None

    
      

    
    def convertToJson(self, response):
        try:
            
            self._buffer = BytesIO()
            response.to_json(self._buffer, orient='records', lines=True, force_ascii=False)
            return self._buffer
        except Exception as e:
            print(f"Erro ao transformar o DF em JSON: {e}")
            self._buffer = None
            return None
    

    def convertToCsv(self, df):
        try:
            self._buffer = BytesIO()
            df.to_csv(self._buffer, index=False)
            self._buffer.seek(0)
            return self._buffer
        except Exception as e:
            print(f"Erro ao transformar o DF em CSV: {e}")
            return None


    def fileName(self, path):
        data_atual = datetime.datetime.now().isoformat()
        match = data_atual.split(".")
        unique_id = str(uuid.uuid4().hex)  # Gera um UUID único como string hexadecimal
        return f"{match[0]}_{unique_id}.csv"
