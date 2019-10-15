from setuptools import setup

setup(
    name='rocks',
    version='0.1.2',
    packages=['rocks'],
    url='https://www.github.com/Lamden/rocks',
    license='GPL3',
    author='Stuart Farmer',
    author_email='stuart@lamden.io',
    description='Long running RocksDB server like Redis Server so that you can '
                'interface with it without worrying about locks and multiprocessing primatives.'
)
