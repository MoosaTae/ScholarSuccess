import os
import json
import requests
import threading
from typing import List, Dict

class ScopusDataScraper:
    def __init__(self, base_dataset_path: str, output_path: str, headers: Dict[str, str]):
        self.base_dataset_path = os.path.abspath(os.path.expanduser(base_dataset_path))
        self.output_path = os.path.abspath(os.path.expanduser(output_path))
        self.headers = headers
        self.lock = threading.Lock()
        os.makedirs(self.output_path, exist_ok=True)

    def process_paths(self, paths: List[str] = None, num_threads: int = 4):
        if not paths:
            paths = [p for p in os.listdir(self.base_dataset_path) 
                     if os.path.isdir(os.path.join(self.base_dataset_path, p)) and '20' in p]
        
        paths.sort(reverse=False)
        print(f"Found paths: {paths}")

        threads = []
        for path in paths[1:]:
            thread = threading.Thread(target=self._process_path, args=(path,))
            thread.start()
            threads.append(thread)

            if len(threads) >= num_threads:
                for t in threads:
                    t.join()
                threads = []


        for t in threads:
            t.join()

    def _process_path(self, path: str):
        """
        Process individual path
        """
        full_path = os.path.join(self.base_dataset_path, path)
        files = os.listdir(full_path)
        
        for file in files:
            file_path = os.path.join(full_path, file)
            with open(file_path, 'r') as f:
                data = json.load(f)
                self._process_file(file, data)

    def _process_file(self, file: str, data: List[Dict]):
        """
        Process individual file and enrich data
        """
        for row in data[:1]:
            try:
                url = f"https://www.scopus.com/gateway/doc-details/documents/{row['eid']}"
                response = requests.get(url, headers=self.headers)
                if response.status_code != 200 :
                    print("++++++++fail get +++++")
                with self.lock:
                    row['abstract'] = response.json().get('abstract', [None])[0] if response.json().get('abstract') else None
                    row['author-keyword'] = response.json().get('authorKeywords', [])
                
                output_file = os.path.join(
                    self.output_path, 
                    f"{file.split('.')[0]}_add.json"
                )   
                print(f"Processed {row['eid']}")
            except Exception as e:
                print(f"Error processing {row.get('eid', 'unknown')}: {e}")
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=4)

def main():
    BASE_DATASET_PATH = '/Users/jp/Desktop/dsde/project2/yok/code/ScrapeDataset'
    OUTPUT_PATH = '/Users/jp/Desktop/dsde/project2/yok/code/abs_ref_data'
    HEADERS = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "cookie": '_fbp=fb.1.1733157383437.91072088876822627; _ga=GA1.1.2025408215.1733157383; _ga_L74H3WXMBD=GS1.1.1733157383.1.1.1733157391.0.0.0; __utma=108693376.2025408215.1733157383.1733158809.1733158809.1; __utmz=108693376.1733158809.1.1.utmcsr=id.elsevier.com|utmccn=(referral)|utmcmd=referral|utmcct=/; Scopus-usage-key=enable-logging; sc_assoc=anon; AT_CONTENT_COOKIE="KDP_FACADE_AFFILIATION_ENABLED:1,KDP_FACADE_ABSTRACT_ENABLED:1,KDP_SOURCE_ENABLED:1,KDP_FACADE_PATENT_ENABLED:1,KDP_FACADE_AUTHOR_ENABLED:1,"; at_check=true; AMCVS_4D6368F454EC41940A4C98A6%40AdobeOrg=1; scopus.machineID=581877FE00808170CEAFD5728C32E78C.i-05b7b6ff82d4adc39; __cfruid=9c609adb6130330078295286df7bfaa1434e8c37-1733402041; _cfuvid=l6S5zPhQRFK6Jbh9rHijJZTYNeCOkXRTzLfY25nBUZY-1733402041361-0.0.1.1-604800000; scopus_key=U12jPZJ0DffXp1WB22KSyyl1; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Dec+05+2024+20%3A37%3A56+GMT%2B0700+(Indochina+Time)&version=202408.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=af3dfe87-1925-4046-a59e-983d03e2e98b&interactionCount=0&isAnonUser=1&groups=1%3A1%2C2%3A1%2C4%3A1%2C3%3A1&geolocation=TH%3B10&landingPath=NotLandingPage&AwaitingReconsent=false; OptanonAlertBoxClosed=2024-12-05T13:37:56.558Z; mbox=PC#cf0cd3191bf74c52908050c849ef860d.38_0#1796650677|session#f704bc7e7ed7497bb1c795e912f12d56#1733407737; AMCV_4D6368F454EC41940A4C98A6%40AdobeOrg=-2121179033%7CMCMID%7C85980083822511365010060777228095980892%7CMCAAMLH-1734013022%7C3%7CMCAAMB-1734013022%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1733415422s%7CNONE%7CMCAID%7CNONE%7CMCCIDH%7C-1385906524%7CvVersion%7C5.3.0%7CMCIDTS%7C20062; s_pers=%20c19%3Dsc%253Arecord%253Adocument%2520record%7C1733410022622%3B%20v68%3D1733405726221%7C1733410022624%3B%20v8%3D1733408246689%7C1828016246689%3B%20v8_s%3DLess%2520than%25201%2520day%7C1733410046689%3B; s_sess=%20s_cpc%3D0%3B%20s_cc%3Dtrue%3B%20e78%3Ddoi%252810.1109%252Fisai-nlp.2018.8692946%2529%3B%20c21%3D0058be7f67553c237542c29822f774cf%3B%20e13%3D05c5753f577ea343a27f09ed02876e54%3B%20c13%3Ddate%2520%2528newest%2529%3B%20s_ppvl%3Dsc%25253Asearch%25253Adocument%252520searchform%252C59%252C59%252C834%252C1440%252C778%252C1440%252C900%252C2%252CP%3B%20e41%3D1%3B%20s_sq%3Delsevier-global-prod%253D%252526c.%252526a.%252526activitymap.%252526page%25253Dsc%2525253Arecord%2525253Adocument%25252520record%252526link%25253DThin%25252520Solid%25252520FilmsVolume%25252520549%2525252C%25252520Pages%25252520292%25252520-%2525252029831%25252520December%252525202013%25252520Document%25252520type%25252520Conference%25252520Paper%25252520Source%25252520type%25252520Journal%25252520ISSN%2525252000406090%25252520DOI%252526region%25253Dprofileleftinside%252526pageIDType%25253D1%252526.activitymap%252526.a%252526.c%252526pid%25253Dsc%2525253Arecord%2525253Adocument%25252520record%252526pidt%25253D1%252526oid%25253Dfunctionkd%25252528%25252529%2525257B%2525257D%252526oidt%25253D2%252526ot%25253DSCOPUS-DOCUMENT-DETAILS-PAGE%3B%20s_ppv%3Dsc%25253Arecord%25253Adocument%252520record%252C6%252C6%252C790.5%252C1440%252C778%252C1440%252C900%252C2%252CP%3B; SCSessionID=31349634F1B2AB660024B584AA70D638.i-05b7b6ff82d4adc39; scopusSessionUUID=686eb220-5e4c-44d0-8; AWSELB=CB9317D502BF07938DE10C841E762B7A33C19AADB1CD1E2BB281380ED740528C8114C6EA236851B4A7650959597186D332103CCE2910BA32070D9964CEACBAE7C5777723B7E0AEAA4E78D91C9073A0D7E2D575C090; __cf_bm=Eff49EtD.FZUkY8D3l3nCgLzc.CphGMi8nSaVBv24iY-1733420608-1.0.1.1-DwFkzRZKHBAbrBHvm3_1wsvGvorYpdNppHxk0qPwgATmGSS3gqGGt2hcj4paVTq.hAMKfkquNhAoleHVwHz8LA; cf_clearance=l4TdzdB1hXCna.tTseCvp01tI2vfnftRlIJvnUatnrg-1733420611-1.2.1.1-BqVDxzHjDmx.NTucKy1uyAU3jOXFc1W55zRt_qVZR1O7SBSO.YpLXHQ_gkxpRnk0_tWwhlXOCJ28WzMrZ3kAqOS5K72gve3dhCocLH4PEDpMRBg92.BNeaNZPxY6.PcaSJr3FNL9MPZdCiAUeCP_XL3bRHFP8cpK2NPtBcEv7C1gG39PTzPUxS1LSCHhFZymSXHNHiIGxNT1upo._eBMEWXhbF0XXW5AL61NF0ywFbCkMRDPKhSCJ3vuwt1swy6pKvFe3cuQvpPvpwgUqmEUWnEAvoeoGMCgVzdoV8dA2.Yz5OVNaW7IIt7RfVXsGmVw8FUp1HBJvabNyoElqUf62N0V88QSkQmB.OP0HrI23k70ZcWi4UAk4Q52ZX5nK2rsqA7v6nFHXekmzbHLtwrHYg; SCOPUS_JWT=eyJraWQiOiJjYTUwODRlNi03M2Y5LTQ0NTUtOWI3Zi1kMjk1M2VkMmRiYmMiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIzNDU2MTI0ODQiLCJkZXBhcnRtZW50SWQiOiI5MDQxODciLCJpc3MiOiJTY29wdXMiLCJpbnN0X2FjY3RfaWQiOiIzMDMxOCIsImlzRXh0ZXJuYWxTdWJzY3JpYmVkRW50aXRsZW1lbnRzIjpmYWxzZSwicGF0aF9jaG9pY2UiOmZhbHNlLCJpbmR2X2lkZW50aXR5IjoiUkVHIiwiZXhwIjoxNzMzNDIxNTE2LCJpYXQiOjE3MzM0MjA2MTcsImVtYWlsIjoiNjYzMzIzOTAyMUBzdHVkZW50LmNodWxhLmFjLnRoIiwiYW5hbHl0aWNzX2luZm8iOnsiYWNjZXNzVHlwZSI6ImFlOlJFRzpVX1A6SU5TVDpTRUxGTUFOUkEiLCJhY2NvdW50SWQiOiIzMDMxOCIsImFjY291bnROYW1lIjoiQ2h1bGFsb25na29ybiBVbml2ZXJzaXR5IiwidXNlcklkIjoiYWU6MzQ1NjEyNDg0In0sImRlcGFydG1lbnROYW1lIjoiUmVtb3RlIGFjY2VzcyIsImluc3RfYWNjdF9uYW1lIjoiQ2h1bGFsb25na29ybiBVbml2ZXJzaXR5Iiwic3Vic2NyaWJlciI6dHJ1ZSwid2ViVXNlcklkIjoiMzQ1NjEyNDg0IiwiaW5zdF9hc3NvY19tZXRob2QiOiJTRUxGTUFOUkEiLCJnaXZlbl9uYW1lIjoiU2FrZGlwYXQiLCJhY2NvdW50TnVtYmVyIjoiQzAwMDAzMDMxOCIsInBhY2thZ2VJZHMiOltdLCJhdWQiOiJTY29wdXMiLCJuYmYiOjE3MzM0MjA2MTcsImZlbmNlcyI6W10sImluZHZfaWRlbnRpdHlfbWV0aG9kIjoiVV9QIiwiaW5zdF9hc3NvYyI6IklOU1QiLCJuYW1lIjoiU2FrZGlwYXQgU3VraGFuZXNrdWwiLCJ1c2FnZVBhdGhJbmZvIjoiKDM0NTYxMjQ4NCxVfDkwNDE4NyxEfDMwMzE4LEF8NSxQfDEsUEwpKFNDT1BVUyxDT058YmRjNzQ3Y2I3Nzc5NTI0ZDA3M2FmM2IxZGIwY2Q4ZDFiNTVlZ3hycWEsU1NPfFJFR19TRUxGTUFOUkEsQUNDRVNTX1RZUEUpIiwicHJpbWFyeUFkbWluUm9sZXMiOltdLCJhdXRoX3Rva2VuIjoiYmRjNzQ3Y2I3Nzc5NTI0ZDA3M2FmM2IxZGIwY2Q4ZDFiNTVlZ3hycWEiLCJmYW1pbHlfbmFtZSI6IlN1a2hhbmVza3VsIn0.CPeacP4REFF4FxbcOa0vA3aOkWzBCldTGWEKxa9NXSXMhS5hB-_jl6vlcW-VoWxyk4wPW-kCYFm0tIBKx6JK1-KuBwdt6oZXARqRFVMqhBW4MMS91xFbGxrLioHZzdvZLtyOqEnNBGqcg2qQdXP8AQ9KqRLbQQVx2WmrGGedrka7-BOUhELwM-I7fWmNlhRw530ROThH54LopFLbkKFNuhq9t-PALFvcoyKQQZ7PtauKRRple_HcBSVJAeCkQSnyPRdkA-Z4wWDDZq7iYIIIT3CQD8rJn3SU_fNnLEwDIEyMQpigH4eR-bE00-z-UD4aixA3I1tb9zf6c_yY9zh3MA; JSESSIONID=6C631C8BBA322B96994EF9CDDCBFEB72'
    }
    
    scraper = ScopusDataScraper(BASE_DATASET_PATH, OUTPUT_PATH, HEADERS)
    scraper.process_paths(num_threads=10)  

if __name__ == "__main__":
    main()