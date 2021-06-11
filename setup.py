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
        "Development Status :: 3 - Alpha",
    ],
    version='0.1.5',
    packages=find_packages(),
    entry_points={
        'console_scripts': ['tangata=tangata.tangata:tangata',
        ],
    },
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'flask',
        'Flask-SocketIO',
        'PyYAML',
        'PyDriller',
        'python-dateutil',
        'simple-websocket',
        'whoosh',
    ],
)