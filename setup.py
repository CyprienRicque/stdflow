from setuptools import find_packages, setup

from stdflow import __version__ as current_version

with open("requirements.txt") as f:
    requirements = f.read().splitlines()


setup(
    name="stdflow",
    version=current_version,
    packages=find_packages(),
    author="Cyprien Ricque",
    author_email="ricque.cyprien@gmail.com",
    description="A package that transform your notebooks and python files into pipeline steps by standardizing the data input / output.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/CyprienRicque/stdflow",
    license="Apache",
    install_requires=requirements,
    classifiers=[
        "Development Status :: First release",
        # 'License :: OSI Approved :: MIT License',
        "Programming Language :: Python :: 3.9",
    ],
)
