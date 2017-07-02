from setuptools import setup

setup(
    name='jobmon',
    packages=['jobmon'],
    entry_points = {
        'console_scripts': 
        ['jobmon = jobmon.runner:main']
    },
    author='Adam Marchetti',
    version='0.6',
    description='Job monitoring system',
    author_email='adamnew123456@gmail.com',
    url='http://github.com/adamnew123456/jobmon',
    keywords=['job', 'monitor'],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: POSIX',
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Development Status :: 3 - Alpha',
        'Topic :: System :: Monitoring'
    ],
    long_description = """
JobMon monitors your jobs, allowing you to start, stop and query them, as
well as receive notifications when they launch or die.
""")
