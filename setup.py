import setuptools
from odtools import VERSION_STR

setuptools.setup(
    name='odtools',
    version=VERSION_STR,
    description='a open-data formatting toolkit for python',
    url='https://github.com/gwappa/python-odtools',
    author='Keisuke Sehara',
    author_email='keisuke.sehara@gmail.com',
    license='MIT',
    install_requires=[
        'stappy>=0.5',
        ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        ],
    packages=['odtools',],
    entry_points={
        # nothing for the time being
    }
)
