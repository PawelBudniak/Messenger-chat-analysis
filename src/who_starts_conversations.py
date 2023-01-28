import pandas as pd
from datetime import datetime
from collections import defaultdict


def count_conversation_starters(df, mins_between_convos=180):
    sender_counts = {}
    prev_timestamp = None

    for index, row in df[::-1].iterrows():
        sender = row["sender_name"]
        timestamp_ms = row["timestamp_ms"]
        timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
        if sender not in sender_counts:
            sender_counts[sender] = 0

        # Check if the current message is part of a continuous conversation
        if prev_timestamp is None or (timestamp - prev_timestamp).total_seconds() > mins_between_convos * 60:
            # The message starts a new conversation
            sender_counts[sender] += 1

        prev_timestamp = timestamp

    return sender_counts

if __name__ == '__main__':
    import stats_pandas as stats
    from pprint import pprint

    #PATH = r'C:\messenger-data\29.12.2022\facebook-100002163210723\messages\inbox\mikolajbienkowski_2348818981812432'
    #df = stats.load_from_path(PATH)

    PATH_TO_CHATS = r'C:\messenger-data\29.12.2022\facebook-100002163210723\messages\inbox'
    df = stats.load_from_chat_name(PATH_TO_CHATS, chat_name='bienkowski')

    pprint(count_conversation_starters(df))
