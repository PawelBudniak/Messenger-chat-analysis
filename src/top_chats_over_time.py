import biggest_chats
import stats_pandas as stats
import pandas as pd


def chat_sizes_by_date(path, *, top_n=-1, freq='sM', filter_groups=False):
    chats = biggest_chats.chat_sizes(path)
    if filter_groups:
        chats = [chat for chat in chats if not chat.is_group]
    chats = chats[:top_n]

    biggest_chats.print_chats(chats)

    chats_dfs = {chat.name: stats.load_from_path(chat.path) for chat in chats}
    grouped = {name: stats.groupby_date(chat_df, frequency=freq) for name, chat_df in chats_dfs.items()}

    big_df = pd.DataFrame()
    for name, msg_by_month in grouped.items():
        data = [msg_by_month.values, msg_by_month.index, [name] * len(msg_by_month)]
        wide_df = pd.DataFrame(data, index=['messages', 'date', 'name']).T
        big_df = pd.concat([big_df, wide_df])

    return pd.pivot(big_df, columns='name', index='date', values='messages').fillna(0)


if __name__ == '__main__':
    import plotly.express as px

    #df = chat_sizes_by_date(r'C:\messenger-data\messages\inbox', top_n=15, filter_groups=False, freq='M')
    df = chat_sizes_by_date(r'C:\messenger-data\29.12.2022\facebook-100002163210723\messages\inbox', top_n=15, filter_groups=False, freq='M')
    f = px.line(df, x=df.index, y=df.columns, labels={'value': 'Message count', 'x': 'Date'})
    f.show()
