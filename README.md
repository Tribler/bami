# BAMI protocol 

[![Tests](https://github.com/grimadas/python-project/workflows/Tests/badge.svg)](https://github.com/grimadas/python-project/actions?workflow=Tests)
[![Codecov](https://codecov.io/gh/grimadas/python-project/branch/master/graph/badge.svg)](https://codecov.io/gh/grimadas/python-project)

 >  BAMI is short for Base Accounting Mechanisms and Interfaces. 

The goal of BAMI is to provide system designers simple tools to build secure ledgers that store valuable information. BAMI has following properties:
* **Tamper-resistance**. BAMI achieves tamper-resistance through organizing information in a chain and by entangling information with each other.
* **Inconsistency resolution**. BAMI has tools to quickly detect delibarate forks and accidental inconsistencies (e.g., as the result of a software bug). BAMI recovers from partitions in the network while ensuring availability and eventual consistency.
* **Reconciliation and Synchronization**. All stored information is organized in groups, which we name communities. A community is identified by a public key and maintains a so-called community chain. Through a robust push-pull gossip algorithm it is guaranteed that peers will eventually received all the information within a single community.


## Data consistency and validity 


The informaiton in BAMI is organised in two types of chains:
 - *Personal chain*: the information created by one peer is kept in the chain, linked with each other. This imposes a sequential order of information by a single writer (see [PRAM](https://jepsen.io/consistency/models/pram)). This is a convenient way to verify and synchronize related data.
 - *DAG representation of a chain*: if a fork is detected in sequential chain we stop interacting with the malicious peer and provide opportunity to fix this inconsistency. Chain as a DAG follows [Causal Consistency](https://jepsen.io/consistency/models/causal).
 
