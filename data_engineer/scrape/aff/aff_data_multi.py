import json
import requests
import threading
from queue import Queue

class AffiliationDataProcessor:
    def __init__(self, input_file, output_file, headers):
        self.input_file = input_file
        self.output_file = output_file
        self.headers = headers
        self.data = []
        self.queue = Queue()
        self.lock = threading.Lock()
        self.count =0

    def load_data(self):
        with open(self.input_file, 'r') as f:
            self.data = json.load(f)

    def fetch_affiliation_data(self):
        while not self.queue.empty():
            row = self.queue.get()
            affid = row['@affid']
            # affid="60005897"
            url = f"https://www.scopus.com/gateway/organisation-profile-api/organizations/{affid}"
            try:
                response = requests.get(url, headers=self.headers)
                if response.status_code != 200:
                    print(response.status_code)
                    print(f"Failed to fetch data for @affid: {affid}")
                    return

                req_json = response.json()
                with self.lock: 
                    row['metrics'] = req_json.get('metrics', {})
                    row['preferredName'] = req_json.get('preferredName', None)
                    row['nameVariants'] = req_json.get('nameVariants', None)
                    row['address'] = req_json.get('address',None)
                    row['hierarchyIds'] = req_json.get('hierarchyIds', None)
                    row['contact'] = req_json.get('contact', None)
                    self.count+=1
            except Exception as e:
                print(f"Error fetching data for @affid: {affid}: {e}")
            finally:
                print(self.count)
                self.queue.task_done()

    def process_data(self, num_threads=5):
        for row in self.data:
            self.queue.put(row)
        threads = []
        for _ in range(num_threads):
            thread = threading.Thread(target=self.fetch_affiliation_data)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    def save_data(self):
        with open(self.output_file, 'w') as f:
            json.dump(self.data, f, indent=4)

    def run(self, num_threads=5):
        self.load_data()
        self.process_data(num_threads=num_threads)
        self.save_data()

if __name__ == "__main__":
    headers = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "cookie": '_fbp=fb.1.1733157383437.91072088876822627; _ga=GA1.1.2025408215.1733157383; _ga_L74H3WXMBD=GS1.1.1733157383.1.1.1733157391.0.0.0; __utma=108693376.2025408215.1733157383.1733158809.1733158809.1; __utmz=108693376.1733158809.1.1.utmcsr=id.elsevier.com|utmccn=(referral)|utmcmd=referral|utmcct=/; Scopus-usage-key=enable-logging; sc_assoc=anon; AT_CONTENT_COOKIE="KDP_FACADE_AFFILIATION_ENABLED:1,KDP_FACADE_ABSTRACT_ENABLED:1,KDP_SOURCE_ENABLED:1,KDP_FACADE_PATENT_ENABLED:1,KDP_FACADE_AUTHOR_ENABLED:1,"; at_check=true; AMCVS_4D6368F454EC41940A4C98A6%40AdobeOrg=1; scopus.machineID=581877FE00808170CEAFD5728C32E78C.i-05b7b6ff82d4adc39; _cfuvid=y5cRfa7cCmEypmC0NmIT.3UR8tL.BBJl8d2PfXUlocw-1733502512292-0.0.1.1-604800000; AMCV_4D6368F454EC41940A4C98A6%40AdobeOrg=-2121179033%7CMCMID%7C85980083822511365010060777228095980892%7CMCAAMLH-1734109804%7C3%7CMCAAMB-1734109804%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1733512204s%7CNONE%7CMCAID%7CNONE%7CMCCIDH%7C-1385906524%7CvVersion%7C5.3.0%7CMCIDTS%7C20064; __cfruid=894e20b864ffb698bf27bf1970622bbdc633ae80-1733505015; s_pers=%20v8%3D1733510085184%7C1828118085184%3B%20v8_s%3DLess%2520than%25201%2520day%7C1733511885184%3B%20c19%3Dsc%253Aresults%253Adocuments%7C1733511885186%3B%20v68%3D1733510075282%7C1733511885189%3B; s_sess=%20s_cpc%3D0%3B%20s_cc%3Dtrue%3B%20e78%3Ddoi%252810.4324%252F9780429501579%2529%3B%20s_ppvl%3Dsc%25253Asearch%25253Aadvanced%252520searchform%252C52%252C41%252C778%252C1440%252C778%252C1440%252C900%252C2%252CP%3B%20c21%3D13e7c46f95304a1f296ced8d8b93c2fe%3B%20e13%3D3558193cd9818af7fe4d2c2f5bd9d00f%3B%20c13%3Ddate%2520%2528newest%2529%3B%20e41%3D1%3B%20s_sq%3Delsevier-global-prod%253D%252526c.%252526a.%252526activitymap.%252526page%25253Dsc%2525253Aresults%2525253Adocuments%252526link%25253DHCI%25252520International%25252520Conference%252525202018%252526region%25253Dcontainer%252526pageIDType%25253D1%252526.activitymap%252526.a%252526.c%252526pid%25253Dsc%2525253Aresults%2525253Adocuments%252526pidt%25253D1%252526oid%25253Dhttps%2525253A%2525252F%2525252Fwww.scopus.com%2525252Frecord%2525252Fdisplay.uri%2525253Feid%2525253D2-s2.0-85064633139%25252526origin%2525253Dresultslist%25252526sort%2525253Dplf-f%25252526src%2525253Ds%252526ot%25253DA%3B%20s_ppv%3Dsc%25253Aresults%25253Adocuments%252C49%252C49%252C1011%252C1440%252C778%252C1440%252C900%252C2%252CP%3B; JSESSIONID=C13E08B73F9FAD07433E113320AA316D; mbox=PC#cf0cd3191bf74c52908050c849ef860d.38_0#1796828910|session#c2729c96d4a74a1e85885308b1ce7ef0#1733585970; OptanonAlertBoxClosed=2024-12-07T15:08:37.317Z; OptanonConsent=isGpcEnabled=0&datestamp=Sat+Dec+07+2024+22%3A08%3A39+GMT%2B0700+(Indochina+Time)&version=202408.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=af3dfe87-1925-4046-a59e-983d03e2e98b&interactionCount=0&isAnonUser=1&groups=1%3A1%2C2%3A1%2C4%3A1%2C3%3A1&geolocation=TH%3B10&landingPath=NotLandingPage&AwaitingReconsent=false; SCSessionID=B245A4B54F2EC4D4E1388BF82954EDB1.i-05b7b6ff82d4adc39; scopusSessionUUID=cffbd912-86ae-4167-8; AWSELB=CB9317D502BF07938DE10C841E762B7A33C19AADB1A4D8CDFEE34411D34772E03914B4EF7BB77B15F7F582A24A8410AF050C39DE3AA31AAC5A6BDE3E4B4DACF34F3854CEEBE0AEAA4E78D91C9073A0D7E2D575C090; SCOPUS_JWT=eyJraWQiOiJjYTUwODRlNi03M2Y5LTQ0NTUtOWI3Zi1kMjk1M2VkMmRiYmMiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIzNDU2MTI0ODQiLCJkZXBhcnRtZW50SWQiOiI5MDQxODciLCJpc3MiOiJTY29wdXMiLCJpbnN0X2FjY3RfaWQiOiIzMDMxOCIsImlzRXh0ZXJuYWxTdWJzY3JpYmVkRW50aXRsZW1lbnRzIjpmYWxzZSwicGF0aF9jaG9pY2UiOmZhbHNlLCJpbmR2X2lkZW50aXR5IjoiUkVHIiwiZXhwIjoxNzMzNTg4NjE0LCJpYXQiOjE3MzM1ODc3MTUsImVtYWlsIjoiNjYzMzIzOTAyMUBzdHVkZW50LmNodWxhLmFjLnRoIiwiYW5hbHl0aWNzX2luZm8iOnsiYWNjZXNzVHlwZSI6ImFlOlJFRzpVX1A6SU5TVDpTRUxGTUFOUkEiLCJhY2NvdW50SWQiOiIzMDMxOCIsImFjY291bnROYW1lIjoiQ2h1bGFsb25na29ybiBVbml2ZXJzaXR5IiwidXNlcklkIjoiYWU6MzQ1NjEyNDg0In0sImRlcGFydG1lbnROYW1lIjoiUmVtb3RlIGFjY2VzcyIsImluc3RfYWNjdF9uYW1lIjoiQ2h1bGFsb25na29ybiBVbml2ZXJzaXR5Iiwic3Vic2NyaWJlciI6dHJ1ZSwid2ViVXNlcklkIjoiMzQ1NjEyNDg0IiwiaW5zdF9hc3NvY19tZXRob2QiOiJTRUxGTUFOUkEiLCJnaXZlbl9uYW1lIjoiU2FrZGlwYXQiLCJhY2NvdW50TnVtYmVyIjoiQzAwMDAzMDMxOCIsInBhY2thZ2VJZHMiOltdLCJhdWQiOiJTY29wdXMiLCJuYmYiOjE3MzM1ODc3MTUsImZlbmNlcyI6W10sImluZHZfaWRlbnRpdHlfbWV0aG9kIjoiIiwiaW5zdF9hc3NvYyI6IklOU1QiLCJuYW1lIjoiU2FrZGlwYXQgU3VraGFuZXNrdWwiLCJ1c2FnZVBhdGhJbmZvIjoiKDM0NTYxMjQ4NCxVfDkwNDE4NyxEfDMwMzE4LEF8NSxQfDEsUEwpKFNDT1BVUyxDT058MDFjZGM2Zjc3MjdhOTU0OTdkOWJiNWMwNzgwODBmZjA3MWEzZ3hycWEsU1NPfFJFR19TRUxGTUFOUkEsQUNDRVNTX1RZUEUpIiwicHJpbWFyeUFkbWluUm9sZXMiOltdLCJhdXRoX3Rva2VuIjoiMDFjZGM2Zjc3MjdhOTU0OTdkOWJiNWMwNzgwODBmZjA3MWEzZ3hycWEiLCJmYW1pbHlfbmFtZSI6IlN1a2hhbmVza3VsIn0.cIQdBa2OnMS3XZc5gx5eigP-Sfx41K_b12J-FIuKMO-XzMGpdXpwj6BfYq5mfYWTKiwF_rjJp-KyLzR_SVXBQBkSUp-VrkAWkV0FmLMpqgqq8mcPhtUy5J_irvQKLFYSUo69f0ugDLMn3Yt_R4PbhxqFFlCGkyjZWV9BqO6ED3hN05xUG6U5Bsn-YrhJc5GU5ADCJ9hPA4xlVbz80T2UC2THTMiPrp9EJuDVivMygNDKsQfTfF9DQxoEJf6KXgEsu-mIUhQlCE0_s-fR_4cZZ9_YnjXpoIizRF3UtHsW2eIltuqKhi4dDw2zvJqyLp1cJ_fzkLBCCyHBZFDhnn_WfQ; __cf_bm=JGlQ9cMEIQtQ9S3gQrMNW8p6nkGPSCU6PUytyLrW5gQ-1733587715-1.0.1.1-mdHOYRQPxsjv18q75aPa4moKqU9Idx4uTKWhUDT3TMi2sUp47B4wxghej36B_PEIdKaX7UPNdS7ClSl6yNJ67g; cf_clearance=G6jcfdruJxjtP2jAR4ck3BsTYgS0tBzxsowsFZGe2b8-1733587717-1.2.1.1-8TvHV_TamtOg4LBtJZmAHixI_dNoVzxOAIEFlFBLjkJDYURk_D2uvDzmSNbXbRxhX8SeU1NPK4tXCiDOGFAsyRGrKaP11ywzGECcwd_EUPQH8711ZgB9rMb5VlRLN.wKRcOHGZlt1x.NEgQH3l8plFw4TsmCnE6dBUFoZWqmiLnIfJEiVjHYIF.GlFXgNSEZxAScoWn9Wh.zq774kD1wSXNn57aoyl78j5hgp1tC8lJ5yKOc0.ND_CMJWsNHiKV7lMOHzocDWcP96qXM2E9yTde4JAEkj9a_zcsnqHFb9fgxP.EPKyH6Q4BflfchXsOiiGmH5alEqUzEDbgYMcTlGOWa1cyU12I3um.VO3zCj1F7ZAFGujNYP7ABy.xXZ4V1AdVAUcURKj2v6hvlfX7F0w' #cookie lifetime is 10minutes
    }
    processor = AffiliationDataProcessor('/Users/jp/Desktop/ScholarSuccess/data_engineer/scrape/aff/all_affid_list.json', '/Users/jp/Desktop/ScholarSuccess/data_engineer/scrape/aff/all_affid_list_add.json', headers)
    processor.run(num_threads=10)  
