from setuptools import setup, find_packages

setup(
    name="StructNoSQL",
    version="2.0.4",
    packages=find_packages(),
    include_package_data=True,
    install_requires=["pydantic", "boto3"],
    url="https://github.com/Robinson04/StructNoSQL",
    license="MIT",
    author="Inoft",
    author_email="robinson@inoft.com",
    description="Structured document based NoSQL client for DynamoDB with automatic data validation and advanced database queries functions.",
)
