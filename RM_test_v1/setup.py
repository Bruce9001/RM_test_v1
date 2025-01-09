import setuptools

setuptools.setup(
    name="RM_test_v1",
    version="1.0.0",
    author="SJKIM",
    author_email="bruce.kim@gbike.io",
    description="RM_test_v1",
    long_description="RM_test_v1",
    long_description_content_type="text/markdown",
    url="https://github.com/Bruce9001/RM_test_v1.git",
    packages=setuptools.find_packages(),
    install_requires=[
        "datetime"

    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    python_requires='>=3.8',

)
