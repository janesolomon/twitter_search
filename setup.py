import os
import re
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


def get_install_requires():
    return [line.strip() for line in read("requirements.txt").split("\n") if line]


setup(
    packages=find_packages(),
    install_requires=get_install_requires()
)
