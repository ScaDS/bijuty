from setuptools import setup, find_packages

setup(
    name="big_data_env_setup",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[], # TODO
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