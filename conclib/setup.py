from setuptools import setup, find_packages

setup(
    name='conclib',
    version='2.0.1',
    description='A library containing the conclib packages.',
    author='',
    author_email='',
    url='https://github.com/uaineteine/concordance_lib',
    packages=find_packages(include=['conclib', 'conclib.*', 'rkeylib', 'rkeylib.*']),
        install_requires=[
        'pyspark',
        'numpy'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.10',
)
