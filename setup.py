from setuptools import setup, find_packages

setup(
    name="RM_test_v1",
    version="1.0.0",
    author="SJKIM",
    author_email="bruce.kim@gbike.io",
    description="RM_test_v1",
    long_description="RM_test_v1",
    long_description_content_type="text/markdown",
    url="https://github.com/Bruce9001/RM_test_v1.git",
    packages=find_packages(),
    install_requires=[
        "datetime>=5.4",
        "pandas>=2.0.0",
        "mysql-connector-python>=8.0.0",
        "numpy>=1.24.0",
        "python-dateutil>=2.8.2",
        "psycopg2-binary>=2.9.0",
        "statsmodels>=0.14.0",
        "scikit-learn>=1.3.0",
        "plotly>=5.13.0"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
    python_requires='>=3.8',
    include_package_data=True,
    zip_safe=False
)