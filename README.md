# BAMI protocol 

[![Tests](https://github.com/grimadas/python-project/workflows/Tests/badge.svg)](https://github.com/grimadas/python-project/actions?workflow=Tests)
[![Codecov](https://codecov.io/gh/grimadas/python-project/branch/master/graph/badge.svg)](https://codecov.io/gh/grimadas/python-project)

 >  BAMI is short for Base Accounting Mechanisms and Interfaces. 

The main ideas for BAMI is to give simple tools to build ledgers for keeping valuable information. BAMI has following properties: 
* **Tamper-resistance**. This is achieved through sharing data organized as a chain and entagling information with each other.
* **Inconsistency resolution**. BAMI has tools to quickly detect delibarate forks and accidental inconsistencies. Finally, it is possible to recover from partitioning while ensuring availability and eventual consistency.
* **Reconciliation and Synchronization**. The information is organized in groups, which we name communites. Community is identified with a public key, as well as an attached to a community chain. Through a robust push-pull gossip it is guaranteed that peer will eventually received all the information from the chain. 


## Data consistency and validity 


The informaiton in BAMI is organised in a number of connected chains: 
 - *Personal chain*: the information created by one peer is kept in the chain, linked with each other. This imposes a sequential on a single writer (see [PRAM](https://jepsen.io/consistency/models/pram)). This is a convenient way to verify, synchronize related data.
 - *DAG representation of a chain*: if there is a fork in a sequential chain we might continue working and give chances to fix inconsistency. Chain as a DAG follows [Causal Consistency](https://jepsen.io/consistency/models/causal). 
 
 





