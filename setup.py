#!/usr/bin/env python

"""Setup script for swallow distributions"""

from setuptools import setup, find_packages
import 

def main():
    setup(
        name = 'swallow',
        version = ElasticSearchLib.__version__,
        description = 'ElasticSearch library for Robot Framework',
        author = 'PagesJaunes',
        author_email = 'fdepaulis@pagesjaunes.fr',
        url = 'https://github.com/pagesjaunes/swallow',
        packages=find_packages(),
        install_requires = ['robotframework','elasticsearch']
    )

if __name__ == "__main__":
    main()