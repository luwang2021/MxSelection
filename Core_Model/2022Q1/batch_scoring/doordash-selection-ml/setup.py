import setuptools

setuptools.setup(
    name='doordash-selection-models-2022Q1batch',
    version='1.0.0',
    author="Lu Wang",
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
