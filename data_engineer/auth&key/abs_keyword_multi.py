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
        for path in paths:
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
        for row in data:
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
    BASE_DATASET_PATH = '/Users/jp/Desktop/ScholarSuccess/data_engineer/auth&key/ScrapeDataset'  # change path
    OUTPUT_PATH = '/Users/jp/Desktop/ScholarSuccess/data_engineer/auth&key/abs_ref_data'  # change path
    HEADERS = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "cookie": '_fbp=fb.1.1733157383437.91072088876822627; _ga=GA1.1.2025408215.1733157383; _ga_L74H3WXMBD=GS1.1.1733157383.1.1.1733157391.0.0.0; __utma=108693376.2025408215.1733157383.1733158809.1733158809.1; __utmz=108693376.1733158809.1.1.utmcsr=id.elsevier.com|utmccn=(referral)|utmcmd=referral|utmcct=/; Scopus-usage-key=enable-logging; sc_assoc=anon; AT_CONTENT_COOKIE="KDP_FACADE_AFFILIATION_ENABLED:1,KDP_FACADE_ABSTRACT_ENABLED:1,KDP_SOURCE_ENABLED:1,KDP_FACADE_PATENT_ENABLED:1,KDP_FACADE_AUTHOR_ENABLED:1,"; at_check=true; AMCVS_4D6368F454EC41940A4C98A6%40AdobeOrg=1; scopus.machineID=581877FE00808170CEAFD5728C32E78C.i-05b7b6ff82d4adc39; _cfuvid=y5cRfa7cCmEypmC0NmIT.3UR8tL.BBJl8d2PfXUlocw-1733502512292-0.0.1.1-604800000; AMCV_4D6368F454EC41940A4C98A6%40AdobeOrg=-2121179033%7CMCMID%7C85980083822511365010060777228095980892%7CMCAAMLH-1734109804%7C3%7CMCAAMB-1734109804%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1733512204s%7CNONE%7CMCAID%7CNONE%7CMCCIDH%7C-1385906524%7CvVersion%7C5.3.0%7CMCIDTS%7C20064; __cfruid=894e20b864ffb698bf27bf1970622bbdc633ae80-1733505015; s_pers=%20v8%3D1733510085184%7C1828118085184%3B%20v8_s%3DLess%2520than%25201%2520day%7C1733511885184%3B%20c19%3Dsc%253Aresults%253Adocuments%7C1733511885186%3B%20v68%3D1733510075282%7C1733511885189%3B; s_sess=%20s_cpc%3D0%3B%20s_cc%3Dtrue%3B%20e78%3Ddoi%252810.4324%252F9780429501579%2529%3B%20s_ppvl%3Dsc%25253Asearch%25253Aadvanced%252520searchform%252C52%252C41%252C778%252C1440%252C778%252C1440%252C900%252C2%252CP%3B%20c21%3D13e7c46f95304a1f296ced8d8b93c2fe%3B%20e13%3D3558193cd9818af7fe4d2c2f5bd9d00f%3B%20c13%3Ddate%2520%2528newest%2529%3B%20e41%3D1%3B%20s_sq%3Delsevier-global-prod%253D%252526c.%252526a.%252526activitymap.%252526page%25253Dsc%2525253Aresults%2525253Adocuments%252526link%25253DHCI%25252520International%25252520Conference%252525202018%252526region%25253Dcontainer%252526pageIDType%25253D1%252526.activitymap%252526.a%252526.c%252526pid%25253Dsc%2525253Aresults%2525253Adocuments%252526pidt%25253D1%252526oid%25253Dhttps%2525253A%2525252F%2525252Fwww.scopus.com%2525252Frecord%2525252Fdisplay.uri%2525253Feid%2525253D2-s2.0-85064633139%25252526origin%2525253Dresultslist%25252526sort%2525253Dplf-f%25252526src%2525253Ds%252526ot%25253DA%3B%20s_ppv%3Dsc%25253Aresults%25253Adocuments%252C49%252C49%252C1011%252C1440%252C778%252C1440%252C900%252C2%252CP%3B; JSESSIONID=C13E08B73F9FAD07433E113320AA316D; mbox=PC#cf0cd3191bf74c52908050c849ef860d.38_0#1796828910|session#c2729c96d4a74a1e85885308b1ce7ef0#1733585970; OptanonAlertBoxClosed=2024-12-07T15:08:37.317Z; OptanonConsent=isGpcEnabled=0&datestamp=Sat+Dec+07+2024+22%3A08%3A39+GMT%2B0700+(Indochina+Time)&version=202408.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=af3dfe87-1925-4046-a59e-983d03e2e98b&interactionCount=0&isAnonUser=1&groups=1%3A1%2C2%3A1%2C4%3A1%2C3%3A1&geolocation=TH%3B10&landingPath=NotLandingPage&AwaitingReconsent=false; SCSessionID=B245A4B54F2EC4D4E1388BF82954EDB1.i-05b7b6ff82d4adc39; scopusSessionUUID=cffbd912-86ae-4167-8; AWSELB=CB9317D502BF07938DE10C841E762B7A33C19AADB1A4D8CDFEE34411D34772E03914B4EF7BB77B15F7F582A24A8410AF050C39DE3AA31AAC5A6BDE3E4B4DACF34F3854CEEBE0AEAA4E78D91C9073A0D7E2D575C090; __cf_bm=Z32aDfHdtiBic9.FCy3RrbMNkhjMzoHBsYa3thNwdLg-1733585122-1.0.1.1-65lxp0.qsWLS6JdB0_L.MKE_LRwFeUrBlUoRtxCqLAUcDmyzHFgqVzEulH6GPFIaExG8OadH8ahV0UaAYVdQOw; cf_clearance=0KEF_jgo1rmIQe0eBWrlHbKnXb1zscVoLWapJob.G0Y-1733585135-1.2.1.1-7M1eE2MQ1hlj0PV1ndottBPpElsg2hjxSLEdrCMbigpr.b80eKv732f37Hm1bK.wozDwmq3m1ZDiFwGYZdyxT7C5Pe7juel7PAgF3IiORZucN4gyviicb5YpPQLtIkbX6lMIDUGUNq4uLGPTTzywP9FUx59rFnh1unW5fOOB39QZgJrrb67Q98NLND36KfKpS.gWUqRIksA3OSDwEpQ9Cc1Sz0pgn_A_6qUxSJU7dw0dfvrwyS5Wu5rlmfPtVAy9vki3_u3N91xvjj1592QjQu9IU_udQzFklKNRtckkieuhuLqreh.8v2MMa12slrSRDyGwhenba_C.Wldz7wdiTJIVeporU0j4R83tY98h17uJWEzETZSqAMUEck_AXfG_jkeyyk0G8XlnOyWWU_H7eg; SCOPUS_JWT=eyJraWQiOiJjYTUwODRlNi03M2Y5LTQ0NTUtOWI3Zi1kMjk1M2VkMmRiYmMiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIzNDU2MTI0ODQiLCJkZXBhcnRtZW50SWQiOiI5MDQxODciLCJpc3MiOiJTY29wdXMiLCJpbnN0X2FjY3RfaWQiOiIzMDMxOCIsImlzRXh0ZXJuYWxTdWJzY3JpYmVkRW50aXRsZW1lbnRzIjpmYWxzZSwicGF0aF9jaG9pY2UiOmZhbHNlLCJpbmR2X2lkZW50aXR5IjoiUkVHIiwiZXhwIjoxNzMzNTg2MDQyLCJpYXQiOjE3MzM1ODUxNDMsImVtYWlsIjoiNjYzMzIzOTAyMUBzdHVkZW50LmNodWxhLmFjLnRoIiwiYW5hbHl0aWNzX2luZm8iOnsiYWNjZXNzVHlwZSI6ImFlOlJFRzpVX1A6SU5TVDpTRUxGTUFOUkEiLCJhY2NvdW50SWQiOiIzMDMxOCIsImFjY291bnROYW1lIjoiQ2h1bGFsb25na29ybiBVbml2ZXJzaXR5IiwidXNlcklkIjoiYWU6MzQ1NjEyNDg0In0sImRlcGFydG1lbnROYW1lIjoiUmVtb3RlIGFjY2VzcyIsImluc3RfYWNjdF9uYW1lIjoiQ2h1bGFsb25na29ybiBVbml2ZXJzaXR5Iiwic3Vic2NyaWJlciI6dHJ1ZSwid2ViVXNlcklkIjoiMzQ1NjEyNDg0IiwiaW5zdF9hc3NvY19tZXRob2QiOiJTRUxGTUFOUkEiLCJnaXZlbl9uYW1lIjoiU2FrZGlwYXQiLCJhY2NvdW50TnVtYmVyIjoiQzAwMDAzMDMxOCIsInBhY2thZ2VJZHMiOltdLCJhdWQiOiJTY29wdXMiLCJuYmYiOjE3MzM1ODUxNDMsImZlbmNlcyI6W10sImluZHZfaWRlbnRpdHlfbWV0aG9kIjoiVV9QIiwiaW5zdF9hc3NvYyI6IklOU1QiLCJuYW1lIjoiU2FrZGlwYXQgU3VraGFuZXNrdWwiLCJ1c2FnZVBhdGhJbmZvIjoiKDM0NTYxMjQ4NCxVfDkwNDE4NyxEfDMwMzE4LEF8NSxQfDEsUEwpKFNDT1BVUyxDT058MDFjZGM2Zjc3MjdhOTU0OTdkOWJiNWMwNzgwODBmZjA3MWEzZ3hycWEsU1NPfFJFR19TRUxGTUFOUkEsQUNDRVNTX1RZUEUpIiwicHJpbWFyeUFkbWluUm9sZXMiOltdLCJhdXRoX3Rva2VuIjoiMDFjZGM2Zjc3MjdhOTU0OTdkOWJiNWMwNzgwODBmZjA3MWEzZ3hycWEiLCJmYW1pbHlfbmFtZSI6IlN1a2hhbmVza3VsIn0.qfzPlHW5DskBCH2ALMl1JOOWlDfZustZp1EMijBRc_AiNcz0pFfvc2Hh-M7r-1DfEV_AMyvt6yYVwJSiyjf4NaRhlD_oJQzJQnW07XKUNGm_-DTkuIPsQbkR0KGhGvIp_uCwU07UiEDaupJFyzySQwIHoiI5II9dq0KS6KPSB1S1xYIMOabzexVobnwLyUj8KZoR9k-kJAyztRNhppdB8SB9sOR5hwgDDuXsLLQ-GRLp6HCsqL4m-fd_gE-N1Zwmu-qArqk_hP4KztXBGtRl1wKedN9uUVdafZhGmAwZ0j5yQmNVF68TS55nxHG-QII0F8oUlgGHHuzNeSqUDoXfJQ'
    }
    
    scraper = ScopusDataScraper(BASE_DATASET_PATH, OUTPUT_PATH, HEADERS)
    scraper.process_paths(num_threads=10)  

if __name__ == "__main__":
    main()