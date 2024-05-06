from s3.connection import Connection
from datetime import datetime
from slugify import slugify
import requests
import json

class KemnakerApi:
    def __init__(self):
        self.s3 = Connection()
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en,id-ID;q=0.9,id;q=0.8,en-US;q=0.7',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Referer': 'https://satudata.kemnaker.go.id/data/kumpulan-data',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }
        self.params = {
            'page': '1',
            'limit': '10',
        }
        self.link = "https://satudata.kemnaker.go.id/data/kumpulan-data"
        self.source = "satu data kemnaker"
        self.domain = self.link.split('/')[2]

    def start(self):
        params = self.params
        headers = self.headers
        page = 1
        
        while True:
            response = requests.get('https://satudata.kemnaker.go.id/api/v1/data/list_data', params=params, headers=headers)
            data = response.json()
            data = data['data']
            if data['data'] == []:
                break
            else:
                for i in range(len(data['data'])):
                    r = requests.get(data['data'][i]['file'])
                    excel = r.content
                    crawling_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    crawling_epoch = int(datetime.timestamp(datetime.now()))
                    kategori = data['data'][i]['ketenagakerjaan']['kategori'].lower()
                    
                    file_excel = f"{slugify(data['data'][i]['judul'])}_{crawling_epoch}.xlsx"
                    self.save_to_excel(local_path=f"output/xlsx/{file_excel}", data=excel)
                    path_s3_excel = f"s3://ai-pipeline-statistics/data/data_raw/data statistic/satu data kemnaker/{kategori}/xlsx/{file_excel}"
                    self.s3.upload(rpath=path_s3_excel, lpath=f"output/xlsx/{file_excel}")
                        
                    raw_data = {
                        "link": self.link,
                        "source": self.source,
                        "domain": self.domain,
                        "path_data_excel": path_s3_excel,
                        "path_data_raw": None,
                        "crawling_time": crawling_time,
                        "crawling_time_epoch": crawling_epoch,
                        "data": data['data'][i],
                    }
                    
                    file_json = f"{crawling_epoch}.json"
                    path_s3_json = f"s3://ai-pipeline-statistics/data/data_raw/data statistic/satu data kemnaker/{kategori}/json/{file_json}"
                    raw_data['path_data_raw'] = path_s3_json
                    
                    self.save_to_json(local_path=f"output/json/{file_json}", data=raw_data)
                    self.s3.upload(rpath=path_s3_json, lpath=f"output/json/{file_json}")

                page += 1
                params['page'] = str(page)
                
    def save_to_json(self, local_path, data):
        with open(local_path, "w") as f:
            json.dump(data, f)
            
    def save_to_excel(self, local_path, data):
        with open(local_path, "wb") as f:
            f.write(data)
        
if __name__ == '__main__':
    kemnaker = KemnakerApi()
    kemnaker.start()