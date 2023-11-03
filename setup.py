from setuptools import setup

setup(
    name="dromedar",
    version="0.1.0",
    description="A package with classes commonly required for creating and querying a database.",
    author="Cedric Stillwater",
    author_email="cedric.stillwater@gmail.com",
    packages=["dromedar"],
    install_requires=[
      "dataset>=1.6.2",
      "psycopg2-binary>=2.9.9"
    ]
)
