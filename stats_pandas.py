import string
import pandas as pd
import calendar
import warnings

class NoReactionsError(Exception): pass

def load_from_path(path):
    import scraper
    s = scraper.Scraper_json()
    return s.scrape_to_df(path)

def filter_nick_changes(chat_df):
    return chat_df[chat_df['content'].map(lambda msg: pd.isna(msg) or ( ('set the nickname' not in msg) and ('set his own nickname' not in msg)))]

def get_msg_stats(chat_df):
    """
    General chat statistics
    
    Arguments:
        chat_df {pd.DataFrame} -- main chat df returned by load_from_path()
    
    Returns:
        dict -- {participant: list[total_message_count, average_message_length, total_characters_sent]}
    """
    msg_stats = {}

    for sender in chat_df['sender_name'].unique():

        messages = chat_df.loc[chat_df['sender_name'] == sender, 'content']
        total_count = len(messages)
        messages = messages.dropna() # filter empty messages 
        txt_msg_count = len(messages)
        avg_msg_len = 0.0
        
        for msg in messages:
            avg_msg_len += len(msg)/txt_msg_count
            
        msg_stats[sender] = [total_count, avg_msg_len, avg_msg_len * txt_msg_count]
    return msg_stats

def get_word_counts(chat_df, filter_participants_names = False, exclude_words = None, min_len = 1):
    """
    Calculate how many times each word has been used by each participant (case insensitive)
    
    Arguments:
        chat_df {pd.DataFrame} -- main chat df returned by load_from_path()
    
    Keyword Arguments:
        filter_participants_names {bool} -- if True every participant name or surname is filtered (they usually clutter the counts) (default: {False})
        exclude_words {Collection} -- strings in the Collection are filtered out (default: {None})
        min_len {int} -- only words higher or equal to min_len are counted (default: {1})
    
    Returns:
        dict -- {participant: dict{word: count}}
    """
    senders = [sender for sender in chat_df['sender_name'].unique()]
    counts ={sender: {} for sender in senders}
    if filter_participants_names: 
        senders_lc = list(map(str.lower, senders)) 

    for index, row in chat_df.iterrows():
        msg = row['content']
        
        if row['type'] == 'Subscribe':
            continue

        if not pd.isna(msg):
            for word in msg.split():
                word = word.strip(string.punctuation)
                word = word.lower()

                if filter_participants_names:
                    if any(word in sender for sender in senders_lc):
                        continue

                if len(word) >= min_len:
                    if word in counts[row['sender_name']]:
                        counts[row['sender_name']][word] += 1
                    else:
                        counts[row['sender_name']][word] = 1

    for sender in senders:
        if not counts[sender]:
            counts.pop(sender) # only include non-empty dictionaries (some may have had only words shorter than the minimal length)

    #sort words by descending counts, only include non-empty dictionaries (some may have had only words shorter than the minimal length)
    counts = {sender: {k:v for k,v in sorted(counts[sender].items(), key = lambda item: item[1], reverse = True)} for sender in counts}
    return counts


def get_msg_types(chat_df, include_txt = False):
    """Calculate how many messages of each type has been sent by each participant
    
    Arguments:
        chat_df {pd.DataFrame} -- main chat df returned by load_from_path()
    
    Keyword Arguments:
        include_txt {bool} -- if True then text messages are included as a type (messes up pie charts) (default: {False})
    
    Returns:
        dict -- {participant: dict{type: count}}
    """
    msg_types = ['photos', 'videos', 'audio_files', 'gifs', 'files', 'sticker', 'share']
    type_counts_by_sender = {}

    for sender in chat_df['sender_name'].unique():
        sender_df = chat_df.loc[chat_df['sender_name'] == sender]
        type_counts = {atype: 0 for atype in msg_types if atype in sender_df and sender_df[atype].count() != 0}

        for atype in msg_types:
            if atype not in type_counts:
                continue
            for alist in sender_df[atype].dropna():
                #it's possible to have multiple multimedia files in a msg, so we want to count all of them
                type_counts[atype] += len(alist)

        if include_txt == True:
            type_counts['txt'] = sender_df['content'].count()

        if type_counts: #ignore senders who have written no messages of the desired types - in their case the type_counts dict is empty
            type_counts_by_sender[sender] = type_counts
    
        
    return type_counts_by_sender

def epoch_to_date(epoch_series, timezone = 'CET'):
    #assumes the timestamp_ms is always in GMT
    dates = pd.to_datetime(epoch_series, unit ='ms')
    dates = dates.apply(lambda d: d.tz_localize(tz = 'GMT'))
    dates = dates.apply(lambda d: d.tz_convert(tz = timezone))
    return dates

def groupby_date(chat_df, frequency = 'M'):
    """
    Groups number of messages by dates, sampling with the specified frequency 
    
    Arguments:
        chat_df {pandas.DataFrame} -- main chat df returned by load_from_path()

    Keyword Arguments:
        frequency {str} -- pandas DateOffset (default: {'M'})
    
    Returns:
        pd.Series -- Series of msg counts indexed by time intervals
    """
    dates = epoch_to_date(chat_df['timestamp_ms'])
    #since groupby groups by index
    dates.index = dates
    dates = dates.groupby(pd.Grouper(freq = frequency)).size()
    return dates   

def groupby_time(chat_df, interval = 'M', interval_names = True):
    """
    Groups number of messages by the specified time interval 
    Different from groupby_date, for example: March 2013 is counted the same as March 2014 here if the interval is specified as 'M' (month)

    Arguments:
        chat_df {pd.DataFrame} -- main chat df returned by load_from_path()
    
    Keyword Arguments:
        interval {str} -- Intervals: 'Y'- year, 'M' - month, 'W' - weekday, 'D' - day, 'H '- hour (default: {'M'})
        interval_names {bool} -- valid for M and W intervals, if True str names are used instead of eg. 0 for Monday (default: {True})
    
    Returns:
        pd.DataFrame  
    """

    interval = interval.upper()
    functions = {'Y': lambda x: x.year, 'M': lambda x: x.month, 'W': lambda x: x.weekday, 'D': lambda x: x.day, 'H': lambda x: x.hour}
    if interval not in functions:
        raise ValueError('Invalid interval')

    times = epoch_to_date(chat_df['timestamp_ms'])
    #since groupby groups by index
    times.index = times
    times = times.groupby(functions[interval]).count()
    if interval == 'M' and interval_names:
        times.index = times.index.map(lambda m: calendar.month_name[m])
    if interval == 'W' and interval_names:
        times.index = times.index.map(lambda d: calendar.day_name[d])
    return times


def word_usage_coefficients(word, word_counts, msg_stats):
    """
    Calculate word_count/message_count for each participant
    
    Arguments:
        word {str} -- 
       word_counts {dict} -- returned from get_word_counts()
        msg_stats {dict} -- returned from get_msg_stats(), used to get message counts

    Returns:
        dict -- participant:coefficient
    """
    coeffs = {}

    for sender in word_counts:
        total = 0
        if word in word_counts[sender]:
            total = word_counts[sender][word]
        n_msgs = msg_stats[sender][0]
        
        if n_msgs != 0: 
            coeffs[sender] = total/n_msgs
    return coeffs

def get_kurwa_coefficients(word_counts, msg_stats, count_related = False):
    """Calculate kurwa_count/message_count for each participant

    Arguments:
        word_counts {dict} -- returned from get_word_counts()
        msg_stats {dict} -- returned from get_msg_stats(), used to get message counts
    
    Keyword Arguments:
        count_related {bool} -- if True related words, such as the plural form, are also counted (default: {False})
    
    Returns:
        dict -- participant:coefficient
    """
    coeffs = {}
    kurwas = {sender: 0 for sender in word_counts}

    for sender in word_counts:
        if count_related is True:
             for word in word_counts[sender]:
                if 'kurw' in word or 'kurew' in word:
                    kurwas[sender] += word_counts[sender][word]

        else:
            if 'kurwa' in word_counts[sender]:
                kurwas[sender] = word_counts[sender]['kurwa']
            else:
                kurwas[sender] = 0
        
        n_msgs = msg_stats[sender][0]

        if n_msgs != 0:
            coeffs[sender] = kurwas[sender]/n_msgs

    return coeffs

def get_profanity_coefficients(word_counts, msg_stats, ignore_kurwas = False):
    """
    Calculate profanity_count/message_count for each participant
    
    Arguments:
        word_counts {dict} -- returned from get_word_counts()
        msg_stats {dict} -- returned from get_msg_stats(), used to get message counts
    
    Keyword Arguments:
        ignore_kurwas {bool} -- if True then every 'kurwa' related word is not counted (default: {False})
    
    Returns:
        dict -- participant:profanity_coefficient
    """
    import profanity
    coeffs = {}

    for sender in word_counts:
        total = 0
        for vulgarism in profanity.profanity:
            
            if ignore_kurwas:
                if 'kurw' in vulgarism or 'kurew' in vulgarism:
                    continue
            if vulgarism in word_counts[sender]:
                total += (word_counts[sender][vulgarism])
        
        n_msgs = msg_stats[sender][0]
        if n_msgs != 0: 
            coeffs[sender] = total/n_msgs
    return coeffs

def total_reacts(chat_df):
    """
    Calculate how often each emoji was used 
    
    Arguments:
        chat_df {pd.DataFrame} -- main chat df returned by load_from_path()
  
    Returns:
        dict -- reaction: count
    """
    if 'reactions' not in chat_df:
        raise NoReactionsError('No reactions have been made in the chat')
    react_types = {}

    for reacts in chat_df['reactions'].dropna():
        for react in reacts:
            emoji = react['reaction']
            if emoji in react_types:
                react_types[emoji] += 1
            else:
                react_types[emoji] = 1
    return react_types

def reaction_stats(chat_df):
    ''' Returns: a tuple (pd.DataFrame - reactions_made, pd.DataFrame - reactions_gotten) '''

    if 'reactions' not in chat_df:
        raise NoReactionsError('No reactions have been made in the chat')

    filtered_df = chat_df.dropna(subset = ['reactions'])
    senders = chat_df['sender_name'].unique()
    reactions_gotten = {sender: {} for sender in senders}
    reactions_made = {actor: {} for actor in senders}

    for index, row in filtered_df.iterrows():
        msg_sender = row['sender_name']
        
        for react in row['reactions']:

            actor = react['actor']
            react_emoji = react['reaction']

            if react_emoji not in reactions_gotten[msg_sender]:
                reactions_gotten[msg_sender][react_emoji] = 1
            else:
                reactions_gotten[msg_sender][react_emoji] += 1
                
            if react_emoji not in reactions_made[actor]:
                reactions_made[actor][react_emoji] = 1
            else:
                reactions_made[actor][react_emoji] += 1
            
    reactions_gotten = pd.DataFrame(reactions_gotten).T.fillna(0)
    reactions_made = pd.DataFrame(reactions_made).T.fillna(0)
    
    n_emojis = len(reactions_gotten.columns)

    reactions_gotten['total'] = reactions_gotten.sum(axis = 1)
    reactions_made['total'] = reactions_made.sum(axis = 1)
    # calculate most frequent emojis for each pearson, [:n_emojis] slice ensures we don't look at the 'total' column
    reactions_gotten['most_received'] = reactions_gotten[reactions_gotten.columns[:n_emojis]].idxmax(axis = 1)
    reactions_made['most_used'] = reactions_made[reactions_made.columns[:n_emojis]].idxmax(axis = 1)

    return (reactions_made, reactions_gotten)
    
def react_percents(df, msg_stats):
    """
    Amount of each reaction received by each participant relative to his message count
    
    Arguments:
        df {pd.DataFrame} -- reactions received df (returned from reaction_stats())
        msg_stats {dict} -- returned from get_msg_stats()
    
    Returns:
        pd.DataFrame
    """
    percents = df.copy()
    for index, row in percents.iterrows():
        n_msgs = msg_stats[index][0]
        percents.loc[index, :-1] = row[:-1]/n_msgs 
    #convert to str with '%' signs, ignore the last 'most_received' column
    percents[percents.columns[:-1]] = percents[percents.columns[:-1]].fillna(0).applymap(lambda x: f'{x:.2%}')
    return percents

def most_reactions(df, title, emoji, percent):
    """Prints the 1st and 2nd person who received most of the specified emoji as a reaction
    
    Arguments:
        df {pd.DataFrame} -- reactions received/percents df
        title {str} -- title for the person, eg. 'funniest' for laughing emoji
        emoji {str} -- one of the facebook reactions in utf-8   
    
    Keyword Arguments:
        percent {bool} -- indicate whether a df with numeric values or str percentages was passed
    """    
    if percent:
        #cast the str percentages to float so that max index can be computed
        emojis = df[emoji].map(lambda x: float(x.rstrip('%'))) 
    else:
        emojis = df[emoji]
    first = emojis.idxmax() 
    second = emojis.drop(index = first).idxmax()

    msg = f'The {title} person is {first}: {df.loc[first, emoji]} of his messages received \'{emoji}\'' \
         + f', 2nd place: {second} ({df.loc[second, emoji]})\n'
    print(msg)    

