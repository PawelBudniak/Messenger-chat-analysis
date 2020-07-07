import os
import json
from collections.abc import Collection, Mapping




def fix_encoding(var):
    """
    Fix broken messenger data encoding into proper utf-8
    (the data was stored in utf-8 and decoded in latin1, so this functions performs a reverse operation)
    
    Arguments:
        var {str or Collection or Mapping} -- [If var is a collection, then the function is applied recursively, trying to find strings to fix, non str or Collection objects are ignored]
    
    Returns:
        str/Collection/Mapping
    """
    try:
        if isinstance(var, str):
            return var.encode('latin1').decode('utf8')
        elif isinstance(var, Mapping):
            return type(var)({fix_encoding(k):fix_encoding(v) for k,v in var.items()})
        elif isinstance(var, Collection):
            return type(var)(fix_encoding(item) for item in var)
    except UnicodeEncodeError as e:
        print('Can\'t decode variable:', var)
        print('Exception msg: ', e)

class Scraper_json:
    def __init__(self):
        self.msg_types = ['photos', 'videos', 'audio_files', 'gifs', 'files', 'sticker', 'share']

    def check_is_dir(self, a_path):
        if not os.path.isdir(a_path):
            raise Exception('Not a directory')
    
    def _get_files(self, path):
        from re import match
        fnames = [file for file in os.listdir(path) if match('message_[0-9]+.json', file)]
        # sort files by their indexes (when sorting by str comparison 10 would be bigger than 2)
        fnames.sort(key = lambda f: int(f[:-5].replace('message_', '')))
        files = [os.path.join(path, file) for file in fnames]
        return files        
    
    def is_garbage_msg(self, msg):
        if msg['type'] == 'Subscribe' or msg['type'] == 'Unsubscribe':
            return True
        return False

    def remove_garbage(self, df):
        indices = df[(df['type'] == 'Subsribe') | (df['type'] == 'Unsubscribe') | (df['type'] == 'Call')].index
        df.drop(columns = 'call duration', inplace = True, errors = 'ignore')
        return df.drop(indices)

    def scrape_to_df(self, path):
        self.check_is_dir(path)
        import pandas as pd

        msg_files = self._get_files(path)
        df = pd.DataFrame()

        for file in msg_files:

            with open(file, encoding='utf-8') as fp:
                file_dict = json.load(fp)
            
            current_df = pd.DataFrame(file_dict['messages'])
            current_df = self.remove_garbage(current_df)

            current_df['content'] = current_df['content'].map(fix_encoding, na_action = 'ignore')
            current_df['sender_name'] = current_df['sender_name'].map(fix_encoding, na_action = 'ignore')
            if 'reactions'  in current_df:
                current_df['reactions'] = current_df['reactions'].map(fix_encoding, na_action = 'ignore')

            df = pd.concat([df, current_df], ignore_index=True, sort = False)

        return df
    
    def scrape_to_csv(self, path, output_path):

        df = self.scrape_to_df(path)
        df.to_csv(output_path, encoding = 'utf-8')




    def _get_msg_type(self, msg_data):
        '''Deprecated '''
        for atype in self.msg_types:

            if atype in msg_data:
                # change typenames with plural forms into singular
                if atype[-1] == 's':
                    atype = atype.rstrip('s')
        
                return atype
        return 'txt'


    def scrape(self, path):
        ''' Deprecated'''
        self.check_is_dir(path)
        msg_files = self._get_files(path)
        chat = {}

        for file in msg_files:
            with open(file, encoding='utf-8') as fp:
                file_dict = json.load(fp)

            for participant in file_dict['participants']:
                #add participants to dict, so we don't have to check for new participants with every message

                # fix broken messenger encoding
                sender = participant['name'].encode('latin1').decode('utf8')

                if sender not in chat:
                    chat[sender] = []

            for msg_data in file_dict['messages']:
                if self.is_garbage_msg(msg_data):
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
        ''' Deprecated '''
        with open(output_path, 'w', encoding='utf-8') as fp:
            json.dump(self.scrape(source_path), fp)
            



if __name__ == "__main__":
    scraper = Scraper_json()
    #scraper.scrape_to_json(R'D:\facebook html i json\facebook-json\DanielSypula_cJS-IpkT2A', 'nowydaniel.json')
    scraper.scrape_to_csv(R'D:\facebook html i json\facebook-json\DanielSypula_cJS-IpkT2A', 'testowy.csv')
