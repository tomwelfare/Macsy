from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="macsy",
    version="0.0.1",
    author="UoB Mediapatterns",
    description="Modular Architecture for Cognitive Systems (macsy)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/uob-mediapatterns/macsy",
    packages=find_packages(exclude=['test']),
    install_requires=[
        'pymongo==3.5.1',
        'python-dateutil',
    ]
)
