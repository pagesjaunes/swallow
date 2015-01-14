#!/usr/bin/env python

"""Setup script for swallow distributions"""

from setuptools import setup, find_packages
import swallow

def main():
    setup(
        name = 'swallow',
        version = swallow.__version__,
        description = 'Swallow for data transformation',
        author = 'PagesJaunes',
        author_email = 'fdepaulis@pagesjaunes.fr',
        url = 'https://github.com/pagesjaunes/swallow',
        packages=find_packages(),
        install_requires = ['algoliasearch','elasticsearch','pymongo']
    )

if __name__ == "__main__":
    main()