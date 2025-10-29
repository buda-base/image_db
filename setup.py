"""
Setup script for BDRC Image Database Tool.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README
readme_file = Path(__file__).parent / 'src' / 'README.md'
long_description = readme_file.read_text() if readme_file.exists() else ''

setup(
    name='image-db-tool',
    version='1.0.0',
    description='BDRC Image Database Tool - High-performance archive indexing',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='BDRC',
    python_requires='>=3.8',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    install_requires=[
        'mysql-connector-python>=8.0.0,<9.0.0',
        'PyYAML>=6.0.0,<7.0.0',
        'Pillow>=10.0.0,<11.0.0',
    ],
    entry_points={
        'console_scripts': [
            'image-db-tool=image_db_tool.cli:main',
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
)
