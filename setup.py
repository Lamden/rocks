from setuptools import setup

setup(
    name='rocks',
    version='0.1.7',
    packages=['rocks'],
    url='https://www.github.com/Lamden/rocks',
    license='GPL3',
    author='Stuart Farmer',
    author_email='stuart@lamden.io',
    description='Long running RocksDB server like Redis Server so that you can '
                'interface with it without worrying about locks and multiprocessing primitives.',
    entry_points='''
        [console_scripts]
        rocks=rocks.__main__:cli
    ''',
    install_requires=['zcomm', 'pyzmq', 'python-rocksdb']
)
