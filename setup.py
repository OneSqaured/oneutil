from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='oneutil',
    version='0.0.2',
    description='OneSquared Utilities',
    long_description=long_description,
    author='Shawn Lin',
    packages=find_packages(),
    python_requires='>=3.6',
    install_requires=[
        'boto3>=1.18.0',
    ],
)
