# SQL SERVER - How to create partition table   

Database: AdventureWorks2022    
Instance: Sql Server 2022

## PARTITIONS.sql   
This scripts shows how to correct create partitioned table   
1. Create partition function with values by witch You can split table to smaller partitions
2. Add filegroup and set them a data files. It gives more chances data will be loaded to correct partition
3. Create partition scheme based on partition function.
4. Create a partition table based on partition scheme and column by witch you can split table to smaller partitions.   
   
   
In the script You will find details about   
- create partition function and schema
- create partition table
- what is range left and right
- add data files and filegroups
- create, truncate and remove any partition
- import data to partition and export data from partition.


## PARTITIONS_ALL_TO_PRIMARY.sql   
This scripts shows how to correct create partitioned table   
1. Create partition function with values by witch You can split table to smaller in one filegroup - PRIMARY
2. Create partition scheme based on partition function.
3. Create a partition table based on partition scheme and column by witch you can split table to smaller partitions.   
   
   
In the script You will find details about   
- create partition function and schema
- create partition table
- what is range left and right
- create, truncate and remove any partition and working on one filegroup - PRIMARY
- import data to partition and export data from partition.  

## PARTITIONS_PROGRAMMABILITY.sql   
This script has sets of functions and procedures to managing partitions in SQL Server. The procedures create and modify partitions functions, schemes and tables only for data loaded to primary filegroup.

More information can be found in sql srcipt, in documantation from my repository   
(Partitions_programmablity_documentation.pdf) and in SQL Server Documentation.