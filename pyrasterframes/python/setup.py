from setuptools import setup, find_packages
import os
import sys

#if sys.version_info < (3, 4):
#    sys.exit("PyRasterFrames requires Python 3.4 or greater.")

with open('README.rst') as f:
    readme = f.read()

setup_args = dict(
    name='pyrasterframes',
    description='Python bindings for RasterFrames',
    long_description=readme,
    version='0.0.1',
    url='http://rasterframes.io',
    author='Simeon H.K. Fitch',
    author_email='fitch@astraea.io',
    license='Apache 2',
    setup_requires=['pytest-runner'],
    install_requires=[ # How is this different from `requirements.txt`?
        'pyspark>=2.1.0,<2.2',
    ],
    tests_require=[
        'pytest==3.4.2'
    ],
    test_suite="pytest-runner",
    packages=['.'] + find_packages(exclude=['tests']),
    include_package_data=True,
    package_data={'.':['LICENSE', 'static/*']},
    exclude_package_data={'.':['setup.cfg']},
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
