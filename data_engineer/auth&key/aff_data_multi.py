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
        "cookie": '_fbp=fb.1.1733157383437.91072088876822627; _ga=GA1.1.2025408215.1733157383; _ga_L74H3WXMBD=GS1.1.1733157383.1.1.1733157391.0.0.0; __utma=108693376.2025408215.1733157383.1733158809.1733158809.1; __utmz=108693376.1733158809.1.1.utmcsr=id.elsevier.com|utmccn=(referral)|utmcmd=referral|utmcct=/; Scopus-usage-key=enable-logging; sc_assoc=anon; AT_CONTENT_COOKIE="KDP_FACADE_AFFILIATION_ENABLED:1,KDP_FACADE_ABSTRACT_ENABLED:1,KDP_SOURCE_ENABLED:1,KDP_FACADE_PATENT_ENABLED:1,KDP_FACADE_AUTHOR_ENABLED:1,"; at_check=true; AMCVS_4D6368F454EC41940A4C98A6%40AdobeOrg=1; _cfuvid=kwfNpX0SHYS2N41zqDbGpq_UoaPxMtqmUYWc0dCkrOE-1733385403914-0.0.1.1-604800000; __cfruid=872ab7ea610ba256aa9c69bbc9f6d162df5ab709-1733385462; AMCV_4D6368F454EC41940A4C98A6%40AdobeOrg=-2121179033%7CMCMID%7C85980083822511365010060777228095980892%7CMCAAMLH-1733991893%7C3%7CMCAAMB-1733991893%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1733394293s%7CNONE%7CMCAID%7CNONE%7CMCCIDH%7C-1385906524%7CvVersion%7C5.3.0%7CMCIDTS%7C20062; cf_clearance=TB4E.b0DtIaYjyz1..CnWqxGstQlqOvrs9fUgLx8Dnw-1733390120-1.2.1.1-n_1.GJjCnnI2T_RySKV1MtPXKn_yvRVHeJ.UlRTX1LsdQat0TH1AQjv5x6qM1lBPxqP4IsfWhfNd2LGHtmwDhN0jIk6p.wpv8tFz9Oxa.EMSYLIBvpZq91ZhIY9kfe_X2JSlpopI_jnfuJ02i3xOrK_woVHHD00DzWofLMtB9597Lz5GMOO0LL6_.RLbtRsyLI9gI56tfi49r.q9l1_tGq7o4jmG.NJlKD_b7KNGCPVgEbFII7s8NCVhEmvx9abYOa26Yoea1i.QLvmBm.rMjP.g6dtbsKnRh6oK4SgFS116LkrNxRJmh7ZJS1wbu.wHkiWtJWgnBhE2ehobXZxZv81lfWhLhHEjfaCPL71exPm8aGFqNJcKh2AeKdztjxGQOjFCOgm2h0NZdcz_CJJ0eg; scopus.machineID=581877FE00808170CEAFD5728C32E78C.i-05b7b6ff82d4adc39; SCSessionID=391117E7D597B0287A9DC405290CAF21.i-05b7b6ff82d4adc39; scopusSessionUUID=db09899a-bb47-47f9-b; AWSELB=CB9317D502BF07938DE10C841E762B7A33C19AADB1D809DF675C1446455FD677A36845A1D83B658A81AB4E1640CE671153542542E6A31AAC5A6BDE3E4B4DACF34F3854CEEBE0AEAA4E78D91C9073A0D7E2D575C090; SCOPUS_JWT=eyJraWQiOiJjYTUwODRlNi03M2Y5LTQ0NTUtOWI3Zi1kMjk1M2VkMmRiYmMiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIzNDU2MTI0ODQiLCJkZXBhcnRtZW50SWQiOiI5MDQxODciLCJpc3MiOiJTY29wdXMiLCJpbnN0X2FjY3RfaWQiOiIzMDMxOCIsImlzRXh0ZXJuYWxTdWJzY3JpYmVkRW50aXRsZW1lbnRzIjpmYWxzZSwicGF0aF9jaG9pY2UiOmZhbHNlLCJpbmR2X2lkZW50aXR5IjoiUkVHIiwiZXhwIjoxNzMzMzkxMDQ2LCJpYXQiOjE3MzMzOTAxNDcsImVtYWlsIjoiNjYzMzIzOTAyMUBzdHVkZW50LmNodWxhLmFjLnRoIiwiYW5hbHl0aWNzX2luZm8iOnsiYWNjZXNzVHlwZSI6ImFlOlJFRzpVX1A6SU5TVDpTRUxGTUFOUkEiLCJhY2NvdW50SWQiOiIzMDMxOCIsImFjY291bnROYW1lIjoiQ2h1bGFsb25na29ybiBVbml2ZXJzaXR5IiwidXNlcklkIjoiYWU6MzQ1NjEyNDg0In0sImRlcGFydG1lbnROYW1lIjoiUmVtb3RlIGFjY2VzcyIsImluc3RfYWNjdF9uYW1lIjoiQ2h1bGFsb25na29ybiBVbml2ZXJzaXR5Iiwic3Vic2NyaWJlciI6dHJ1ZSwid2ViVXNlcklkIjoiMzQ1NjEyNDg0IiwiaW5zdF9hc3NvY19tZXRob2QiOiJTRUxGTUFOUkEiLCJnaXZlbl9uYW1lIjoiU2FrZGlwYXQiLCJhY2NvdW50TnVtYmVyIjoiQzAwMDAzMDMxOCIsInBhY2thZ2VJZHMiOltdLCJhdWQiOiJTY29wdXMiLCJuYmYiOjE3MzMzOTAxNDcsImZlbmNlcyI6W10sImluZHZfaWRlbnRpdHlfbWV0aG9kIjoiVV9QIiwiaW5zdF9hc3NvYyI6IklOU1QiLCJuYW1lIjoiU2FrZGlwYXQgU3VraGFuZXNrdWwiLCJ1c2FnZVBhdGhJbmZvIjoiKDM0NTYxMjQ4NCxVfDkwNDE4NyxEfDMwMzE4LEF8NSxQfDEsUEwpKFNDT1BVUyxDT058NDc4OGJjZGI1ZDlkOTg0YWQ5N2JjYWUwNGVmNWFjMjdmNjdhZ3hycWEsU1NPfFJFR19TRUxGTUFOUkEsQUNDRVNTX1RZUEUpIiwicHJpbWFyeUFkbWluUm9sZXMiOltdLCJhdXRoX3Rva2VuIjoiNDc4OGJjZGI1ZDlkOTg0YWQ5N2JjYWUwNGVmNWFjMjdmNjdhZ3hycWEiLCJmYW1pbHlfbmFtZSI6IlN1a2hhbmVza3VsIn0.BNNu1dYds2_SFV15YeqVj-1Ns4cVFbFldjNAdfBo8a_uJTnScRqYRaMfAyIuHj_ZIk_bemZ5sRCsp8lKIucW9viHfgV8kzsqCvonTvYa763S06IstLLjMxZoQblMSM4-4ypQXto0TB89vmY_CuqzZXcGUGOsWo_CDSOVsbmZAkyaTcY2gIAq9EF5RlFw3hYq9fAvlwgiex8DISNuCPxpj7FLEddUXY1B0fLDXzI5YrdJBR4Sb3bXV1fDD8mxj2H4_bGLke-ouP0k2wwug69n44ONnowD-OyI31mdnrCzGmJj2yapdlovuTp_ovGxu7HrKq18aVTlaXuqTEYm4gdBeQ; scopus_key=wvUK4RAHsbV5GZsc4iXwh2lu; __cf_bm=YrBfWNL57USwy50QmCiNgjAUuF6aMBksTjaj.F6OsZc-1733390158-1.0.1.1-.EhdwuTg83kyyY6gWN02d.WlzCqfaKnG.WANG.XdO.eoBjGRlU7GPJ_Wjg3KZEEoQSTUUlcW2HX4xRReBodfxQ; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Dec+05+2024+16%3A19%3A40+GMT%2B0700+(Indochina+Time)&version=202408.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=af3dfe87-1925-4046-a59e-983d03e2e98b&interactionCount=0&isAnonUser=1&groups=1%3A1%2C2%3A1%2C4%3A1%2C3%3A1&geolocation=TH%3B10&landingPath=NotLandingPage&AwaitingReconsent=false; OptanonAlertBoxClosed=2024-12-05T09:19:40.064Z; mbox=PC#cf0cd3191bf74c52908050c849ef860d.38_0#1796635181|session#e8d7f69f387445b19a69b5450f53803c#1733392241; s_pers=%20c19%3Dsc%253Asearch%253Adocument%2520searchform%7C1733392181425%3B%20v68%3D1733390379895%7C1733392181430%3B%20v8%3D1733390387488%7C1827998387488%3B%20v8_s%3DLess%2520than%25207%2520days%7C1733392187488%3B; JSESSIONID=B3074DA24CEFE46CB842C5FD0D5A3FB4; s_sess=%20s_cpc%3D0%3B%20s_cc%3Dtrue%3B%20e78%3Ddoi%252810.1109%252Fisai-nlp.2018.8692946%2529%3B%20c21%3D0058be7f67553c237542c29822f774cf%3B%20e13%3D05c5753f577ea343a27f09ed02876e54%3B%20c13%3Ddate%2520%2528newest%2529%3B%20e41%3D1%3B%20s_sq%3Delsevier-global-prod%253D%252526c.%252526a.%252526activitymap.%252526page%25253Dsc%2525253Asearch%2525253Adocument%25252520searchform%252526link%25253DSearch%252526region%25253Dbasic-panel%252526pageIDType%25253D1%252526.activitymap%252526.a%252526.c%252526pid%25253Dsc%2525253Asearch%2525253Adocument%25252520searchform%252526pidt%25253D1%252526oid%25253Dfunctionkd%25252528%25252529%2525257B%2525257D%252526oidt%25253D2%252526ot%25253DSUBMIT%3B%20s_ppvl%3Dsc%25253Asearch%25253Adocument%252520searchform%252C59%252C59%252C834%252C1440%252C778%252C1440%252C900%252C2%252CP%3B%20s_ppv%3Dsc%25253Arecord%25253Adocument%252520record%252C6%252C6%252C790.5%252C1440%252C778%252C1440%252C900%252C2%252CP%3B'
    }
    processor = AffiliationDataProcessor('/Users/jp/Desktop/ScholarSuccess/data_engineer/auth&key/all_affid_list.json', '/Users/jp/Desktop/ScholarSuccess/data_engineer/auth&key/all_affid_list2.json', headers)
    processor.run(num_threads=10)  
