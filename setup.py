import sys

try:
    from setuptools import setup, find_packages
    use_setuptools = True
except ImportError:
    from distutils.core import setup
    use_setuptools = False

try:
    with open('README.rst', 'rt') as readme:
        description = '\n' + readme.read()
except IOError:
    # maybe running setup.py from some other dir
    description = ''


python_requires = '>=3.6'
install_requires = [
    'distogram>=2.0',
    'python-dateutil>=2.8',
    'Rx>=3.1.1',
]

setup(
    name="rxsci",
    version='0.15.0',
    url='https://github.com/maki-nage/rxsci.git',
    license='MIT',
    description="ReactiveX for data science",
    long_description=description,
    author='Romain Picard',
    author_email='romain.picard@oakbits.com',
    packages=find_packages(),
    install_requires=install_requires,
    platforms='any',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
    ],
    project_urls={
        'Documentation': 'https://rxsci.readthedocs.io',
    }
)
