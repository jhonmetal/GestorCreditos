from setuptools import setup, find_packages
 
setup(
    name='leadsAnalytics',
    version='1.0.0',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    package_data={
        '': ['log4j.properties'],
    },
    install_requires=[
        'pyspark==3.5.3',
        'pytz>=2024.2',
        'pandas==2.2.3',
        'setuptools==75.3.0',
    ],
    description='Spark ETL process for medallion architecture',
    author='Jhonathan Pauca Joya',
    author_email='jhonathan.pauca@unmsm.com.pe',
    url='https://github.com/jhonmetal/leadsrecover',
    entry_points={
        'console_scripts': [
            'run_pipeline=analytics.elt_job:main',
        ],
    },
)