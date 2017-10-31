from setuptools import setup, find_packages
import sys

if sys.version_info < (3, 4):
    sys.exit("PyRasterFrames requires Python 3.4 or greater.")


def readme():
    with open('README.rst') as f:
        return f.read()


setup_args = dict(
    name='pyrasterframes',
    description='Python bindings for RasterFrames',
    long_description=readme(),
    version='0.0.1',
    url='http://rasterframes.io',
    packages=find_packages(),
    author='Simeon H.K. Fitch',
    author_email='fitch@astraea.io',
    license='Apache 2',
    install_requires=[
        'pyspark>=2.1.0,<2.2',
        'py4j==0.10.4'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Other Environment',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Software Development :: Libraries'
    ],
    zip_safe=False
    # entry_points={
    #     "console_scripts": ['pyrasterframes=pyrasterframes:console']
    # }
)

if __name__ == "__main__":
    setup(**setup_args)
