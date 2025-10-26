#%%
from setuptools import setup, find_packages
import os

def parse_requirements(filename):
    with open(filename, encoding="utf-8") as f:
        lines = f.read().splitlines()
    # Strip comments and empty lines
    return [line.strip() for line in lines if line.strip() and not line.startswith("#")]
#%%
setup(
    name="midwest_heritage",
    version="0.1.0",
    description="A package supporting the Midwest Heritage bidding database",
    author="Austin J Brandenberger",
    author_email="austin.brandenberger@gmail.com",
    packages=find_packages(),
    install_requires=parse_requirements(
        os.path.join(os.path.dirname(__file__), 'requirements.txt')
        ),
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)