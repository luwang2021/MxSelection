import setuptools

setuptools.setup(
    name='doordash-selection-models-batch',
    version='1.0.0',
    author="Chen Dong",
    author_email="chen.dong@doordash.com",
    description='Selection ml models',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6"
)
