from setuptools import find_packages, setup

setup(
    name='tangata',
    url='tangata.chrisjenkins.nz',
    author="Chris Jenkins",
    author_email='chris@chrisjenkins.nz',
    version='0.0.1',
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
    ],
)