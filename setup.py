from setuptools import setup, find_packages

setup(
    name="oneutil",
    version="0.0.3",
    description="OneSquared Utilities",
    long_description=open("README.md", "r").read(),
    long_description_content_type="text/markdown",
    author="Shawn Lin",
    packages=find_packages(),
    python_requires=">=3.6",
    install_requires=[
        "boto3>=1.18.0",
    ],
)
