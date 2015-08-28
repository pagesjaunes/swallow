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
        install_requires = ["algoliasearch >= 1.5.0, < 2","elasticsearch >= 1.5.0, < 2","pymongo >= 3.0.0, < 4","PyMySQL >= 0.6.6, < 1"]
    )

if __name__ == "__main__":
    main()