'''
counting the same things as in stats.py but using pandas dataframe interface instead of dictionaries 
'''
import string
import pandas as pd

def get_msg_stats(chat_df):
    msg_stats = {}

    for sender in chat_df['sender_name'].unique():

        messages = chat_df.loc[chat_df['sender_name'] == sender]['content']
        total_count = len(messages)
        messages.dropna(inplace = True) # filter empty messages 
        txt_msg_count = len(messages)
        avg_msg_len = 0.0
        
        for msg in messages:
            avg_msg_len += len(msg)/txt_msg_count
            
        msg_stats[sender] = [total_count, avg_msg_len, avg_msg_len * txt_msg_count]
    return msg_stats

def get_word_counts(chat_df):
    counts ={sender: {} for sender in chat_df['sender_name'].unique()}

    for index, row in chat_df.iterrows():
        msg = row['content']

        if not pd.isna(msg):
            for word in msg.split():
                word = word.strip(string.punctuation)
                word = word.lower()
                if word in counts[row['sender_name']]:
                    counts[row['sender_name']][word] += 1
                else:
                    counts[row['sender_name']][word] = 1
    return counts