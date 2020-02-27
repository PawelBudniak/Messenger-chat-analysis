import json
import string

#click, video, audio do wyjebania, jsonem najlepiej?

JSON_STATS = R'data/skladczarnuchow.json'

# def word_counter(astring):
#     word_count = {}
#     for word in astring.split():
#         word.strip(string.punctuation)
#         word.lower()
#         if word not in word_count:
#             word_count[word] = 1
#         else:
#             word_count[word] += 1
#     return word_count
def word_counter_msgs(messages, forbidden_words = None):
    word_count = {}
    for msg in messages:
        for word in msg[0].split():
            # ~ is reserved for special msgs - video or audio, strip every other punctuation sign
            word = word.strip(string.punctuation.replace('~',''))
            if '~' not in word:
                word = word.lower()
            if forbidden_words:
                if word in forbidden_words:
                    break
            if word not in word_count:
                word_count[word] = 1
            else:
                word_count[word] += 1
    #sort by descending counts
    word_count = {k: v for k,v in sorted(word_count.items(), key = lambda item: item[1], reverse = True)}
    return word_count

def get_msg_stats(chat_stats):
    '''
    Return: dict {sender: [msg_count, avg_msg_len, total_characters_sent]}
    '''
    msg_stats = {}

    for sender, messages in chat_stats.items():
        total_msgs = len(messages)
        avg_msg_len = 0.0
        for msg in messages:
            avg_msg_len += len(msg[0])/total_msgs
        msg_stats[sender] = [len(messages), avg_msg_len, avg_msg_len * total_msgs]

    return msg_stats

def get_word_counts(chat_stats, filter_participants_names = False, exclude_words = None):
    '''
    Return: dict {sender1: {word1: count1, word2: count2...}, sender2: {...}}
    '''
    forbidden_words = ''
    if exclude_words is not None:
        forbidden_words += ' '. join(exclude_words).lower()
    if filter_participants_names:
        forbidden_words += ' '.join(chat_stats.keys()).lower()
    word_counts = {}
    for sender,messages in chat_stats.items():
            word_counts[sender] = word_counter_msgs(messages, forbidden_words)
    return word_counts

def load_json(json_file):
    '''
    Return: dict {sender1: [ [msg1,date1], [msg2,date2] ... ] }
    '''
    with open(json_file, 'r', encoding='utf-8') as fp:
        chat_stats = json.load(fp)
    return chat_stats

def get_kurwa_coefficients(word_counts, msg_stats):
    coeffs = {}
    for sender in word_counts:
        #kurwas per message
        if 'kurwa' in word_counts[sender]:
            coeffs[sender] = "{:.1%}".format(word_counts[sender]['kurwa']/msg_stats[sender][0])
    return coeffs

def get_profanity_coefficients(word_counts, msg_stats):
    coeffs = {}
    for sender in word_counts:
        pass


def load_from_path(path):
    '''
    Return: dict {sender1: [ [msg1,date1], [msg2,date2] ... ], sender2: [...], ... }
    '''
    import scraper
    return scraper.scrape(path)
        
       
if __name__ == "__main__":

    # with open(JSON_STATS, 'r', encoding='utf-8') as fp:
    #     chat_stats structure: dict {sender: (message, date)}
    #     chat_stats = json.load(fp)

    # word_counts = get_word_counts(chat_stats)
    # for sender, stats in word_counts.items():
    #     i = 1
    #     print(f'\n~~~~~~~~~{sender}:~~~~~~~~~~')
    #     for word, count in stats.items():
    #         if len(word) >= 7:
    #             print(f'{i}. {word}: {count}', end ='  ')
    #             i += 1
    #             if i > 15:
    #                 break
    import seaborn as sns
    import matplotlib.pyplot as plt
    import pandas as pd

    # df = pd.DataFrame.from_dict(msg_lens, orient='index', columns =['counts'])
    # df.columns = ['senders', 'counts']
    # print(df.head())
    # df = pd.DataFrame.from_dict(msg_lens)
    # print(df.head())
    chat_stats = load_json(JSON_STATS)
    msg_stats = get_msg_stats(chat_stats)

    df = pd.DataFrame(((k,*v) for k,v in msg_stats.items()))
    df.columns = ['Sender', 'Total msgs sent', 'Avg msg length', 'Total chars sent']
    print(df.head())

    sns.set(rc={'figure.figsize':(10,10)})
    ax = sns.barplot(x='Sender', y='Total msgs sent', data=df)
    ax.set_xticklabels(ax.get_xticklabels(), rotation =35)
    plt.tight_layout()
    plt.savefig('tescik')
    plt.show()

        
        
# i = 0
# for k,v in word_counts['Bartek Królak'].items():
#     print(f'{i}. {k} : {v}')
#     i += 1
#     if i > 30:
#         break



# msg_stats = get_msg_stats(chat_stats)
# for sender, stats in msg_stats.items():
#     print(f'{sender}: {stats}')

# import pandas as pd





#kazdy typek w nowej kolumnie: 
#df = pd.DataFrame.from_dict(msg_stats)
# print(df.head())

# for key, value in chat_stats.items():
#     print(f'{key}: {len(value)}')

# characters_sent ={}
# msg_lens = {sender: len(msgs) for sender,msgs in chat_stats.items()}
# msg_lens = {k:v for k,v in sorted(msg_lens.items(), key = lambda item: item[1]) }

# for sender, messages in chat_stats.items():
#     total_msgs = len(messages)
#     avg_msg_len = 0.0
#     for msg in messages:
#         avg_msg_len += len(msg[0])/total_msgs
#     print(f'{sender}: average msg length: {avg_msg_len}')
#     print(f'so, total characters sent:{total_msgs * avg_msg_len}')
#     characters_sent[sender] = avg_msg_len * total_msgs

# sorter = {key: value for (key, value) in sorted(characters_sent.items(), key = lambda item: item[1])}
# for k,v in sorter.items():
#     print(f'{k}: {v}')  

# for k,v in msg_lens.items():
#     print(f'{k}: {v}')  

# import seaborn as sns
# import matplotlib.pyplot as plt
# import pandas as pd

# # df = pd.DataFrame.from_dict(msg_lens, orient='index', columns =['counts'])
# # df.columns = ['senders', 'counts']
# # print(df.head())
# # df = pd.DataFrame.from_dict(msg_lens)
# # print(df.head())
# df = pd.DataFrame(msg_lens.items(), columns = ['Sender', 'Message Count'])
# print(df.head())

# sns.set(rc={'figure.figsize':(10,10)})
# ax = sns.barplot(x='Sender', y='Message Count', data=df)
# ax.set_xticklabels(ax.get_xticklabels(), rotation =35)
# plt.tight_layout()
# plt.savefig('tescik')
# plt.show()



