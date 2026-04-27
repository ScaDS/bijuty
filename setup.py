from setuptools import setup, find_packages

setup(
    name="big_data_utils",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "ipywidgets>=8.0.0",
        "ipython>=8.0.0",
        "psutil>=5.9.0",
        "requests>=2.28.0",
        "plotly>=5.0.0",
    ],
    author="Apurv Deepak Kulkarni",
    author_email="your.email@example.com",
    description="A simple Python module contianing tools to setup big data framework environment in jupyterhub.",
    license="GNU V3",
    url="https://gitlab.hrz.tu-chemnitz.de/apku868a--tu-dresden.de/big-data-environment-setup-for-jupyterhub",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU V3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.11.5',
)
