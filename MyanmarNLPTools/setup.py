import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='MyanmarNLPTools',
    version='0.0.1',
    author='Htet Aung@Ted',
    author_email='htetaung04@gmail.com',
    description='Myanmar NLP Tools',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='#',
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    include_package_data=True,
    python_requires='>=3.6',
)
