from setuptools import setup

setup(
    name='model_cache',
    version='0.0.4',
    url='http://github.com/17zuoye/model_cache/',
    license='MIT',
    author='David Chen',
    author_email=''.join(reversed("moc.liamg@emojvm")),
    description='model_cache',
    long_description='model_cache',
    packages=['model_cache', 'model_cache/storage'],
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    install_requires=[
        'redis_collections',
        'sqlitedict',
        'etl_utils',
        'forwardable.py == 0.0.11',
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
)
