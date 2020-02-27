import os
from bs4 import BeautifulSoup

MSG_PATH = R"D:\facebook-100002163210723\skladczarnuchow_9ZWCplP1iw"
MSG_PATH2 = R"D:\facebook-100002163210723\TomekTrzeciak_kbNqf2UuEg"
# for root, dirs, files in os.walk(MSG_PATH): #in os.listdir(MSG_PATH):
#     for file in files:
#         if '.' not in file:
#             print(file)

# for file in os.listdir(MSG_PATH):
#     if '.' not in file:
#             print(file)

def add_to_dict(adict, div_msg):
    contents = div_msg.contents
    if len(contents) != 3:
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

def scrape(path):
    msg_files = [os.path.join(path, file) for file in os.listdir(path) if '.html' in file]
    messages ={}

    for file in msg_files:
        with open(file, encoding='utf-8') as fp:
            soup = BeautifulSoup(fp.read())
            divs = soup.find_all('div', {'class': 'pam _3-95 _2pi0 _2lej uiBoxWhite noborder'})
            for div in divs:
                add_to_dict(messages, div)
    
    return messages

def scrape_to_json(source_path, output_path):
    import json
    
    with open(output_path) as fp:
        json.dump(scrape(source_path), fp)


if __name__ == "__main__":
    pass