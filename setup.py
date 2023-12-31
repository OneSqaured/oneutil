from setuptools import setup, find_packages

setup(
    name="oneutil",
    version="0.0.4",
    description="OneSquared Utilities",
    long_description=open("README.md", "r").read(),
    long_description_content_type="text/markdown",
    author="Shawn Lin",
    packages=find_packages(),
    python_requires=">=3.6",
    install_requires=[
        "boto3>=1.18.0",
        "databento>=0.16.0",
    ],
)
