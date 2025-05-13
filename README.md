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
