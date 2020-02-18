from rocks.server import RocksDBServer
from rocks.client import RocksDBClient
from rocks import constants
import asyncio

import argparse


def print_logo():
    print('''
                                      _
                            .-.      / \        _
                ^^         /   \    /^./\__   _/ \\
              _        .--'\/\_ \__/.      \ /    \  ^^  ___
             / \_    _/ ^      \/  __  :'   /\/\  /\  __/   \\
            /    \  /    .'   _/  /  \   ^ /    \/  \/ .`'\_/\\
           /\/\  /\/ :' __  ^/  ^/    `--./.'  ^  `-.\ _    _:\ _
          /    \/  \  _/  \-' __/.' ^ _   \_   .'\   _/ \ .  __/ \\
        /\  .-   `. \/     \ / -.   _/ \ -. `_/   \ /    `._/  ^  \\
       /  `-.__ ^   / .-'.--'    . /    `--./ .-'  `-.  `-. `.  -  `.
     @/        `.  / /      `-.   /  .-'   / .   .'   \    \  \  .-  \%
     @(88%@)@%% @)&@&(88&@.-_=_-=_-=_-=_-=_.8@% &@&&8(8%@%8)(8@%8 8%@)%
     @88:::&(&8&&8::JGS:&`.~-_~~-~~_~-~_~-~~=.'@(&%::::%@8&8)::&#@8::::
     `::::::8%@@%:::::@%&8:`.=~~-.~~-.~~=..~'8::::::::&@8:::::&8::::::'
      `::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::'
                    ______  _____  _______ _     _ _______
                   |_____/ |     | |       |____/  |______
                   |    \_ |_____| |_____  |    \_ ______| 2
                                            
    ''')


def cli():
    parser = argparse.ArgumentParser(description="Rocks Commands", prog='rocks')
    parser.add_argument('command', type=str)

    parser.add_argument('-k', '--key', type=str)
    parser.add_argument('-v', '--value', type=str)
    parser.add_argument('-d', '--dir', type=str, default=None)

    args = parser.parse_args()

    if args.command == 'serve':
        print('Serving Rocks...')
        f = args.dir or constants.DEFAULT_DIRECTORY

        s = RocksDBServer(filename=f)

        print_logo()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(s.serve())
    elif args.command == 'get':
        key = args.key.encode()

        c = RocksDBClient()

        v = c.get(key)
        if v is not None:
            print(v.decode())
        else:
            print(v)
    elif args.command == 'set':
        key = args.key.encode()
        value = args.value.encode()

        c = RocksDBClient()
        c.set(key, value)
        print('OK')
    elif args.command == 'delete':
        key = args.key.encode()

        c = RocksDBClient()
        c.delete(key)
        print('OK')
    else:
        print('Unsupported command: {}'.format(args.command))


if __name__ == '__main__':
    cli()