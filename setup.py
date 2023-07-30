from setuptools import setup, find_packages

setup(
    name='oneutil',
    version='0.0.1',
    description='OneSquared Utilities',
    author='Shawn Lin',
    packages=find_packages(),
    install_requires=[
        'boto3>=1.18.0',
    ],
)
