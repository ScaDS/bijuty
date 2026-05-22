from setuptools import setup, find_packages

setup(
    name="bijuty",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "ipywidgets>=8.0.0",
        "ipython>=8.0.0",
        "psutil>=5.9.0",
        "requests>=2.28.0",
        "plotly>=5.0.0",
        "anywidget>=0.11.0"
    ],
    author="Apurv Deepak Kulkarni",
    author_email="apurv.kulkarni@tu-dresden.de",
    description="An interactive dashboard for manageing big data cluster lifecycle on jupyterhub running on HPC system.",
    license="GPL-3.0-or-later",
    url="https://github.com/apurvkulkarni7/bijuty",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.11.5',
)
