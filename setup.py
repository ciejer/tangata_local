from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='tangata',
    url='https://github.com/ciejer/tangata_local',
    author="Chris Jenkins",
    author_email='chris@chrisjenkins.nz',
    description='An interactive catalog for dbt_',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Database :: Front-Ends",
        "Framework :: Flask",
        "Development Status :: 4 - Beta",
    ],
    version='0.2.1',
    packages=find_packages(),
    entry_points={
        'console_scripts': ['tangata=tangata.tangata:tangata',
        ],
    },
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'simple-websocket==0.2.0',
        'Flask==1.1.2',
        'Flask_SocketIO==5.0.3',
        'Whoosh==2.7.4',
        'APScheduler==3.7.0',
        'GitPython==3.1.18',
        'python_dateutil==2.8.1',
        'PyYAML==5.4.1',
        'ruamel.base==1.0.0',
    ],
)