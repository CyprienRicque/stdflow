from setuptools import setup, find_packages

setup(
    name="stdflow",
    version="0.0.27",
    description="[alpha] A package that transform your notebooks and python files into pipeline steps by standardizing the data input / output.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Cyprien Ricque",
    author_email="ricque.cyprien@gmail.com",
    py_modules=["stdflow"],
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        'License :: OSI Approved :: MIT License',
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.9",
    ],
    packages=find_packages(),
    install_requires=[
        "pandas>=2.0.0; python_version>='3.9'",
        "typing-extensions>=4.7.1; python_version<'3.8'",
        "pandas~=1.3.5; python_version<'3.8'",
        "openpyxl~=3.1.2",
        "python-box[all]~=7.0",
        "ipynbname",
        "nbconvert"
    ],
    keywords=["data science", "data", "flow", "data flow"],
    zip_safe=False,
)
