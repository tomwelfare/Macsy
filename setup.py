from setuptools import setup

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
    packages=["macsy"],
    install_requires=[
        'pymongo==2.7.0',
        'automodinit',
	'python-dateutil',
    ]
)
