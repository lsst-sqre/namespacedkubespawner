from __future__ import print_function
from setuptools import setup, find_packages

setup(
    name='namespacedkubespawner',
    version='0.0.7',
    install_requires=[
        'jupyterhub-kubespawner>=0.10',
    ],
    python_requires='>=3.5',
    description='Namespaced Spawner for Kubernetes',
    url='http://github.com/lsst-sqre/namespacedkubespawner',
    author='Adam Thornton',
    author_email='athornton@lsst.org',
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    license='MIT',
    packages=find_packages(),
    project_urls={
        'Source': 'https://github.com/lsst-sqre/namespacedkubespawner',
        'Tracker': 'https://github.com/lsst-sqre/namespacedkubespawner/issues',
    },
)
