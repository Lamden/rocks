from rocks.server import RocksDBServer
from rocks import constants
import asyncio
import click


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
                   |    \_ |_____| |_____  |    \_ ______|
                                            
    ''')


@click.group()
def cli():
    pass


@cli.command()
@click.option('-d', '--dir', type=str)
def serve(dir):
    f = dir or constants.DEFAULT_DIRECTORY

    s = RocksDBServer(filename=f)

    print_logo()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(s.serve())


@cli.command()
@click.option('-k', '--key', type=str, required=True)
@click.option('-d', '--dir', type=str)
def get(key, dir):
    f = dir or constants.DEFAULT_DIRECTORY

    key = key.encode()

    s = RocksDBServer(filename=f)

    v = s.db.get(key)
    if v is not None:
        print(v.decode())
    else:
        print(v)


@cli.command()
@click.option('-k', '--key', type=str, required=True)
@click.option('-v', '--value', type=str, required=True)
@click.option('-d', '--dir', type=str)
def set(key, value, dir):
    f = dir or constants.DEFAULT_DIRECTORY

    key = key.encode()
    value = value.encode()

    s = RocksDBServer(filename=f)
    s.db.put(key, value)
    print('OK')


@cli.command()
@click.option('-k', '--key', type=str, required=True)
@click.option('-d', '--dir', type=str)
def delete(key, dir):
    f = dir or constants.DEFAULT_DIRECTORY

    key = key.encode()

    s = RocksDBServer(filename=f)
    s.db.delete(key)
    print('OK')


if __name__ == '__main__':
    cli()