# BAMI protocol 🍜 

[![Tests](https://github.com/grimadas/python-project/workflows/Tests/badge.svg)](https://github.com/grimadas/python-project/actions?workflow=Tests)
[![Codecov](https://codecov.io/gh/grimadas/python-project/branch/master/graph/badge.svg)](https://codecov.io/gh/grimadas/python-project)

 >  BAMI is short for Base Accounting Mechanisms and Interfaces. 

The goal of BAMI is to provide system designers simple tools to build secure ledgers that store valuable information. BAMI has following properties:
* **Tamper-resistance**. BAMI achieves tamper-resistance through organizing information in a chain and by entangling information with each other.
* **Inconsistency resolution**. BAMI has tools to quickly detect delibarate forks and accidental inconsistencies (e.g., as the result of a software bug). BAMI recovers from partitions in the network while ensuring availability and eventual consistency.
* **Reconciliation and Synchronization**. All stored information is organized in groups, which we name communities. A community is identified by a public key and maintains a so-called community chain. Through a robust push-pull gossip algorithm it is guaranteed that peers will eventually received all the information within a single community.

# Installation
In order to build and run the project, we advise you to use `poetry` in combination with Python 3.18. To install `poetry`, follow the instructions on [their website](https://python-poetry.org/docs/#installation). Once `poetry` is installed, you can install the project dependencies by running `poetry install` in the root of the project. This will install all the dependencies in a virtual environment. You can then run the example simulation by running `example.py` located in the simulation package.

# Jupyter notebook
For this course, we provide you with instructions in the form of a Jupyter notebook. You can start the notebook by running `poetry run jupyter notebook` in the root of the project. This will open a browser window with the notebook. You can then run the cells in the notebook by pressing `Shift + Enter`.
