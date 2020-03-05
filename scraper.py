import os
from bs4 import BeautifulSoup
import json

MSG_PATH = R"D:\facebook-100002163210723\skladczarnuchow_9ZWCplP1iw"
MSG_PATH2 = R"D:\facebook-100002163210723\TomekTrzeciak_kbNqf2UuEg"
# for root, dirs, files in os.walk(MSG_PATH): #in os.listdir(MSG_PATH):
#     for file in files:
#         if '.' not in file:
#             print(file)

# for file in os.listdir(MSG_PATH):
#     if '.' not in file:
#             print(file)

class Scraper_html:

    def _add_to_dict(self, adict, div_msg):
        contents = div_msg.contents
        if len(contents) < 3:
            print('nie 3')
            return
        sender = contents[0].text
        msg = contents[1].text
        date = contents[2].text

        #messages that include files such as photos or audios are read as empty strings, so they are ignored here
        if msg:
            if msg == "Click for video:":
                msg = "~VIDEO"
            if msg == "Click for audio:":
                msg = "~AUDIO"
            if sender not in adict:
                adict[sender] = [(msg, date)]
            else:
                adict[sender].append((msg, date))

    def scrape(self, path):
        msg_files = [os.path.join(path, file) for file in os.listdir(path) if '.html' in file]
        messages ={}

        for file in msg_files:
            with open(file, encoding='utf-8') as fp:
                soup = BeautifulSoup(fp.read())
                divs = soup.find_all('div', {'class': 'pam _3-95 _2pi0 _2lej uiBoxWhite noborder'})
                for div in divs:
                    self._add_to_dict(messages, div)
        
        return messages

    def scrape_to_json(self, source_path, output_path):

        with open(output_path, 'w', encoding='utf-8') as fp:
            json.dump(self.scrape(source_path), fp)


class Scraper_json:
    def __init__(self):
        self.msg_types = ['photos', 'videos', 'audio_files', 'gifs', 'files', 'sticker', 'share']

    def _get_msg_type(self, msg_data):
        for atype in self.msg_types:

            if atype in msg_data:
                # change typenames with plural forms into singular
                if atype[-1] == 's':
                    atype = atype.rstrip('s')
        
                return atype
        return 'txt'
        

    def scrape(self, path):
        msg_files = [os.path.join(path, file) for file in os.listdir(path) if '.json' in file]
        chat = {}

        for file in msg_files:
            with open(file, encoding='utf-8') as fp:
                file_dict = json.load(fp)

            for participant in file_dict['participants']:
                #add participants to dict, so we don't have to check for new participants with every message

                # fix broken messenger encoding
                #TODO: explain
                sender = participant['name'].encode('latin1').decode('utf8')

                if sender not in chat:
                    chat[sender] = []

            for msg_data in file_dict['messages']:
                if msg_data['type'] == 'Subscribe' or msg_data['type'] == 'Unsubscribe':
                    continue

                date = msg_data['timestamp_ms']
                msg_type = self._get_msg_type(msg_data)

                # fix broken messenger encoding
                if 'content' in msg_data and msg_data['type'] == 'Generic':
                    txt = msg_data['content'].encode('latin1').decode('utf8')
                else:
                    txt = ''

                sender = msg_data['sender_name'].encode('latin1').decode('utf8')
                
                chat[sender].append((txt, date, msg_type))
        return chat
            

    
    def scrape_to_json(self, source_path, output_path):
        with open(output_path, 'w', encoding='utf-8') as fp:
            json.dump(self.scrape(source_path), fp)

    def scrape_to_df(self, path):  
        import pandas as pd

        with open(path) as fp:
            fdict = json.load(fp)

        for p in fdict['participants']:
            p['name'] = p['name'].encode('latin1').decode('utf8')

        for msg in fdict['messages']:
            if 'content' in msg:
                msg['content'] = msg['content'].encode('latin1').decode('utf8')
            msg['sender_name'] = msg['sender_name'].encode('latin1').decode('utf8')

        return  pd.DataFrame(fdict['messages'])

                





if __name__ == "__main__":
    scraper = Scraper_json()
    scraper.scrape_to_json(R'D:\facebook html i json\facebook-json\DanielSypula_cJS-IpkT2A', 'nowydaniel.json')