#!/usr/bin/env python

from setuptools import setup
from setuptools import find_packages
from os.path import join as opj
from os.path import dirname


def get_version():
    """Load version only
    """
    with open(opj(dirname(__file__), 'ants_seg_to_nidm', '__init__.py')) as f:
        version_lines = list(filter(lambda x: x.startswith('__version__'), f))
    assert (len(version_lines) == 1)
    return version_lines[0].split('=')[1].strip(" '\"\t\n")

# extension version
version = get_version()
PACKAGES = find_packages()

README = opj(dirname(__file__), 'README.md')
try:
    import pypandoc
    long_description = pypandoc.convert(README, 'rst')
except (ImportError, OSError) as exc:
    print(
        "WARNING: pypandoc failed to import or threw an error while converting"
        " README.md to RST: %r  .md version will be used as is" %exc
    )
    long_description = open(README).read()

# Metadata
setup(
    name='ants_seg_to_nidm',
    version=version,
    description='ANTS segmentation data to NIDM / jsonld',
    long_description=long_description,
    author='David Keator',
    author_email='dbkeator@uci.edu',
    url='https://github.com/ReproNim/ants_seg_to_nidm',
    packages=PACKAGES,
    install_requires=[
        'nibabel',
        'numpy',
        # The converter runs on PyNIDM 4.5 and 5 (ants_seg_to_nidm/_pynidm.py
        # picks the API). Applications that install it pin the exact version.
        'pynidm>=4.5.0,<6',
        'pandas',
        # prov 3.0.0 moved NetworkX graph interop to an optional extra that
        # PyNIDM 4.x's nidm.experiment imports; the converter also uses
        # prov.model directly, which PyNIDM 5 no longer pulls in.
        'prov[graph]',
        # Loose on purpose: PyNIDM 4.5.0 declares rdflib~=6.3.2 and PyNIDM 5
        # declares rdflib>=6.3.2,<8, so PyNIDM chooses the rdflib version.
        'rdflib>=6.0',
        'requests',
    ], # Add requirements as necessary
    include_package_data=True,
    extras_require={
        'devel-docs': [
            # for converting README.md -> .rst for long description
            'pypandoc',
        ]},
    entry_points={
        'console_scripts': [
            'antsegstats2nidm=ants_seg_to_nidm.ants_seg_to_nidm:main' # this is where the console entry points are defined
            ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ], # Change if necessary
)
