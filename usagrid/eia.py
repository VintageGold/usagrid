from dataclasses import dataclass
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import requests
import math
from usagrid import s3

@dataclass
class EIA:
    start_date:str
    end_date:str
    api_key:str
    length:int=5000

    def get_response(self,url:str,params:dict) -> pd.DataFrame:
            
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
        else:
            print(f"Error: {response.status_code}")
            return response
        
        df = pd.DataFrame(data["response"]["data"])
        
        return df

    def _process_response(self,response) -> pd.DataFrame:

        if response.status_code == 200:
            data = response.json()
        else:
            print(f"Error: {response.status_code}")
            return response

        print(data["response"]["total"])

        chunks = range(0,int(data["response"]["total"]),self.length)

        return chunks


    def get_balancingauthority(self,test:bool=True):

        total_records = 0
        
        url = "https://api.eia.gov/v2/electricity/rto/daily-region-data/data/"
        
        params = {
                    "frequency": "daily",
                    "data[0]": "value",
                    "facets[type][]": ["D", "DF", "NG", "TI"],
                    "sort[0][column]": "period",
                    "sort[0][direction]": "asc",
                    "offset": 0,
                    "length": self.length,
                    "start": self.start_date,
                    "end": self.end_date,
                    "api_key":self.api_key
                }
        
        response = requests.get(url,params=params)
        
        chunks = self._process_response(response)

        print(chunks)

        if test:
            chunks_pbar = tqdm(chunks[:1],leave=True)

        else:
            chunks_pbar = tqdm(chunks[:],leave=True)

        for i in chunks_pbar:

            params["offset"] = i

            df = self.get_response(url,params=params)

            end_date = df["period"].max()

            total_records += df.shape[0]

            start_label = f"End Date {end_date} and total records {total_records}"
            
            chunks_pbar.set_description(start_label)

            df_response = df.assign(period=pd.to_datetime(df["period"]))

            start_date = str(df_response.period.min().date())
            end_date = str(df_response.period.max().date())

            fn=f"landingarea/balancing_authority/start_date_{start_date}_end_date_{end_date}_balancing_authority.arrow"

            s3.write_data_to_s3_pyarrow(bucket_name="usagrid",object_key=fn,data=df_response)

        assert i <= total_records

        return df

    def get_powerplant(self,test:bool=True):

        total_records = 0

        url = "https://api.eia.gov/v2/electricity/facility-fuel/data/"



        data = ["average-heat-content","consumption-for-eg",
                        "consumption-for-eg-btu",
                        "generation",
                        "gross-generation",
                        "total-consumption",
                        "total-consumption-btu"]
        params = {
            "frequency": "monthly",
            "facets[type][]": [],
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "offset": 0,
            "length": self.length,
            "start": self.start_date,
            "end": self.end_date,
            "api_key":self.api_key
        }

        for i,d in enumerate(data):

            params[f"data[{i}]"] = d


        response = requests.get(url, params=params)

        chunks = self._process_response(response)

        print(chunks)

        if test:
            chunks_pbar = tqdm(chunks[:1],leave=True)

        else:
            chunks_pbar = tqdm(chunks[:],leave=True)

        for i in chunks_pbar:

            params["offset"] = i

            df = self.get_response(url,params=params)

            end_date = df["period"].max()

            total_records += df.shape[0]

            start_label = f"End Date {end_date} and total records {total_records}"
            
            chunks_pbar.set_description(start_label)

            df_response = df.assign(period=pd.to_datetime(df["period"]))

            start_date = str(df_response.period.min().date())
            end_date = str(df_response.period.max().date())

            fn=f"landingarea/powerplants/start_date_{start_date}_end_date_{end_date}_powerplants.arrow"

            s3.write_data_to_s3_pyarrow(bucket_name="usagrid",object_key=fn,data=df_response)

        assert i <= total_records

        return df




        # https://api.eia.gov/v2/electricity/facility-fuel/data/