from setuptools import setup

setup(
    name='exchange_rates_cnb',
    version='1.0',
    description='Import exchange rates from CNB api to database postgresql',
    author='Martin Divis',
    author_email='martin.divis@accentue.com',
    keywords='echange, rates',
    url='https',
    py_modules=['exchange_rates_cnb'],
    install_requires=['requests-oauthlib==1.3.1','pandoc==2.3','psycopg2==2.9.9','workalendar==17.0.0','python-dateutil==2.8.2'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: Public Domain',
        'Operating System :: Microsoft :: Windows :: Windows 11',
        'Programming Language :: Python :: 3.9',
        'Topic :: Software Development :: Libraries',
        ],
    zip_safe=False,
)