import os
import json
import sys
from collections.abc import Collection, Mapping
from dataclasses import dataclass

howmany = 20


def fix_encoding(var):
    """
    Fix broken messenger data encoding into proper utf-8
    (the data was stored in utf-8 and decoded in latin1, so this functions performs a reverse operation)
    
    Arguments:
        var {str or Collection} -- [If var is a collection, then the function is applied recursively, trying to find strings to fix, non str or Collection objects are ignored]
    
    Returns:
        [str or Colletion] 
    """
    if isinstance(var, str):
        return var.encode('latin1').decode('utf8')
    elif isinstance(var, Mapping):
        return type(var)({fix_encoding(k): fix_encoding(v) for k, v in var.items()})
    elif isinstance(var, Collection):
        return type(var)(fix_encoding(item) for item in var)


def get_files(path):
    from re import match
    files = [os.path.join(path, file) for file in os.listdir(path) if match('message_[0-9]+.json', file)]
    return files


def usage():
    print('Usage: py biggest_chats.py <inbox directory path> <optional: how many chats>')
    exit()


@dataclass
class Chat:
    path: str
    message_count: int
    name: str


def chat_sizes(path):
    """
    Returns: {str: chat_name -> int: message_count}
    """
    chats = []

    try:
        subfolders = (f.path for f in os.scandir(path) if f.is_dir())
    except FileNotFoundError as e:
        print('Error:', e)

    for subfolder in subfolders:
        msg_files = get_files(subfolder)
        chat = None

        for file in msg_files:
            with open(file, encoding='utf-8') as fp:
                file_dict = json.load(fp)
            chat_name = file_dict['title']
            chat_name = fix_encoding(chat_name)
            chat_size = len(file_dict['messages'])
            if chat is None:
                chat = Chat(path=subfolder, message_count=chat_size, name=chat_name)
            else:
                chat.message_count += chat_size

        chats.append(chat)

    return sorted(chats, key=lambda c: c.message_count)



def print_chats(chats):
    for i, chat in enumerate(chats):
        print(f'{i+1}. {chat.name}: {chat.message_count}')


def main():
    if len(sys.argv) == 2:
        path = sys.argv[1]
    elif len(sys.argv) == 3:
        path = sys.argv[1]
        try:
            howmany = int(sys.argv[2])
        except ValueError as e:
            print('Error:', e)
            print(
                'If your path contains spaces you have to enclose it in single apostrophes like this: \'my path/chat\'')
            usage()
    else:
        usage()

    chats = chat_sizes(path)
    print_chats(chats[:howmany])


if __name__ == '__main__':
    main()
