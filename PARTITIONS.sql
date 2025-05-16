-------------------------------------------------------------------------
-- Copyright (c) 2025 Kacper Prusiñski

-- This is example of partitions table management.
-- The script describe how to:
--		create partition function and schema
--		create partition table
--		what is range left and right
--		add data files and filegroups
--		create, truncate and remove any partition
--		import data to partition and export data from partition

-- SQL Server 2022
-- AdventureWorks2022

-- Author script: Kacper Prusinski
-- Script name: PARTITIONS.sql
-------------------------------------------------------------------------

USE AdventureWorks2022
GO

-- 1) CREATE PARTITION FUNCTION WITH TEMPLATE VALUES FOR PARTITIONS
IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'PartitionByYear')
CREATE PARTITION FUNCTION PartitionByYear(DATE)
AS RANGE LEFT FOR VALUES ('2023-12-31', '2024-12-31', '2025-12-31', '2026-12-31');

/*
	RANGE LEFT:
	1. <= 2023-12-31
	2. > 2023-12-31, <= 2024-12-31
	3. > 2024-12-31, <= 2025-12-31
	4. > 2025-12-31, <= 2026-12-31

	RANGE RIGHT:
	1. < 2023-12-31
	2. >= 2023-12-31, < 2024-12-31
	3. >= 2024-12-31, < 2025-12-31
	4. >= 2025-12-31, < 2026-12-31
*/

-- list of partition functions in the database
SELECT NAME, FUNCTION_ID, TYPE, TYPE_DESC, BOUNDARY_VALUE_ON_RIGHT
FROM sys.partition_functions;


-- 2) ADD FILEGROUP FOR ANY PARTITION
IF NOT EXISTS (SELECT 1 FROM sys.filegroups WHERE name = 'FG_2023')
ALTER DATABASE AdventureWorks2022 ADD FILEGROUP FG_2023;

IF NOT EXISTS (SELECT 1 FROM sys.filegroups WHERE name = 'FG_2024')
ALTER DATABASE AdventureWorks2022 ADD FILEGROUP FG_2024;

IF NOT EXISTS (SELECT 1 FROM sys.filegroups WHERE name = 'FG_2025')
ALTER DATABASE AdventureWorks2022 ADD FILEGROUP FG_2025;

IF NOT EXISTS (SELECT 1 FROM sys.filegroups WHERE name = 'FG_2026')
ALTER DATABASE AdventureWorks2022 ADD FILEGROUP FG_2026;


-- list of filegroups in database
SELECT * FROM sys.filegroups;

-- 3) CREATE DATA FILES FOR PARTICULAR FILE GROUP
-- .mdf - Master Data File
-- .ndf - Next(secondary) Data File
ALTER DATABASE AdventureWorks2022 ADD FILE (
	NAME = P_2023,
	FILENAME = 'D:\sql_server_2022\MSSQL16.MSSQLSERVER\MSSQL\DATA\P_2023.ndf' -- your fullpath
) TO FILEGROUP FG_2023;

ALTER DATABASE AdventureWorks2022 ADD FILE (
	NAME = P_2024,
	FILENAME = 'D:\sql_server_2022\MSSQL16.MSSQLSERVER\MSSQL\DATA\P_2024.ndf' -- your fullpath
) TO FILEGROUP FG_2024;

ALTER DATABASE AdventureWorks2022 ADD FILE (
	NAME = P_2025,
	FILENAME = 'D:\sql_server_2022\MSSQL16.MSSQLSERVER\MSSQL\DATA\P_2025.ndf' -- your fullpath
) TO FILEGROUP FG_2025;

ALTER DATABASE AdventureWorks2022 ADD FILE (
	NAME = P_2026,
	FILENAME = 'D:\sql_server_2022\MSSQL16.MSSQLSERVER\MSSQL\DATA\P_2026.ndf' -- your fullpath
) TO FILEGROUP FG_2026;

-- list of file groups, file name/partition name, full path to file and its size 
SELECT 
	fg.name AS FileGroupName, mf.name AS LogicalFileName,
	mf.physical_name AS PhysicalFilePatch, mf.size / 128 AS SizeInMB
FROM sys.filegroups fg
INNER JOIN sys.master_files mf ON fg.data_space_id = mf.data_space_id
WHERE mf.database_id = DB_ID('AdventureWorks2022');

-- 4) CREATE PARTITION SCHEME BASED ON FILE GROUPS
-- DATA NOT MATCHING WITH ALL FILEGROUPS WILL FALL INTO DEFAULT PARTIONED "PRIMARY"

-- CREATE PARTITION SCHEME SchemePartitionByYear
-- AS PARTITION PartitionByYear
-- TO (FG_2023, FG_2024, FG_2025, FG_2026);

-- if previous query return error: Msg 7707, Level 16, State 1, Line 68
CREATE PARTITION SCHEME SchemePartitionByYear
AS PARTITION PartitionByYear
TO (FG_2023, FG_2024, FG_2025, FG_2026, [PRIMARY]);


-- 5) CREATE PARTITIONED TABLE BASED ON PARTITION SCHEME
CREATE TABLE Sales.Orders_Partitioned(
	OrderID int,
	Orderdate Date,
	Sales int
) ON SchemePartitionByYear(OrderDate);

/*
SchemePartitionByYear - partition scheme
OrderDate - column by which we partition this table
*/

-- 6) INSERTING DATA
INSERT INTO Sales.Orders_Partitioned VALUES
	(1,  '2023-05-15', 100),
	(2,  '2024-07-20', 100),
	(3,  '2025-07-20', 60),
	(4,  '2026-07-21', 200), 
	(5,  '2023-07-15', 100),
	(6,  '2024-11-19', 100),
	(7,  '2025-12-20', 60), 
	(8,  '2026-01-21', 200), 
	(9,  '2023-02-15', 100), 
	(10, '2024-04-20', 100), 
	(11, '2025-09-13', 60),
	(12, '2026-07-04', 200); 

-- get all data from table Sales.Orders_Partitioned and partition number
SELECT op.*, $Partition.PartitionByYear(Orderdate) as PartitionNumber 
FROM Sales.Orders_Partitioned op;

-- get partition number, name and records quantity 
SELECT p.partition_number, fg.name, p.rows
FROM sys.partitions p
INNER JOIN sys.destination_data_spaces dds ON p.partition_number = dds.destination_id
INNER JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
WHERE OBJECT_NAME(p.object_id) = 'Orders_Partitioned';


-- 7) ADD NEW FILE GROUPS AND PARTITIONS TO EXISTING TABLE

-- a) add new file group 
ALTER DATABASE AdventureWorks2022 ADD FILEGROUP FG_2022;

-- b) add new data file
ALTER DATABASE AdventureWorks2022 ADD FILE (
	NAME = P_2022,
	FILENAME = 'D:\sql_server_2022\MSSQL16.MSSQLSERVER\MSSQL\DATA\P_2022.ndf' -- your fullpath
) TO FILEGROUP FG_2022;

-- c) alter scheme - point to new partition 
ALTER PARTITION SCHEME SchemePartitionByYear
NEXT USED FG_2022;

-- d) alter partition function - split new range from neighbour partition range
-- split '2022-12-31' from '2023-12-31'
ALTER PARTITION FUNCTION PartitionByYear()
SPLIT RANGE ('2022-12-31');

-- e) insert data
INSERT INTO Sales.Orders_Partitioned VALUES
	(13, '2022-05-15', 300), 
	(14, '2022-07-20', 200), 
	(15, '2022-07-20', 107);


-- analogical for partitions FG_2027 and FG_2028

-- FG_2027
ALTER DATABASE AdventureWorks2022 ADD FILEGROUP FG_2027;

ALTER DATABASE AdventureWorks2022 ADD FILE (
	NAME = P_2027,
	FILENAME = 'D:\sql_server_2022\MSSQL16.MSSQLSERVER\MSSQL\DATA\P_2027.ndf' -- your fullpath
) TO FILEGROUP FG_2027;

ALTER PARTITION SCHEME SchemePartitionByYear
NEXT USED FG_2027;

ALTER PARTITION FUNCTION PartitionByYear()
SPLIT RANGE ('2027-12-31');

INSERT INTO Sales.Orders_Partitioned VALUES
	(16, '2027-08-21', 200), 
	(17, '2027-08-23', 200),
	(18, '2027-08-23', 200);


-- FG_2028
ALTER DATABASE AdventureWorks2022 ADD FILEGROUP FG_2028;

ALTER DATABASE AdventureWorks2022 ADD FILE (
	NAME = P_2028,
	FILENAME = 'D:\sql_server_2022\MSSQL16.MSSQLSERVER\MSSQL\DATA\P_2028.ndf' -- your fullpath
) TO FILEGROUP FG_2028;

ALTER PARTITION SCHEME SchemePartitionByYear
NEXT USED FG_2028;

ALTER PARTITION FUNCTION PartitionByYear()
SPLIT RANGE ('2028-12-31');

INSERT INTO Sales.Orders_Partitioned VALUES
	(19, '2028-08-21', 200),
	(20, '2028-10-12', 200),
	(21, '2028-03-01', 200);

-- get all data from table Sales.Orders_Partitioned and partition number
SELECT op.*, $Partition.PartitionByYear(Orderdate) as PartitionNumber 
FROM Sales.Orders_Partitioned op;

-- get partition number, name and records quantity 
SELECT p.partition_number, fg.name, p.rows
FROM sys.partitions p
INNER JOIN sys.destination_data_spaces dds ON p.partition_number = dds.destination_id
INNER JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
WHERE OBJECT_NAME(p.object_id) = 'Orders_Partitioned';

-- all info about partitions in table "Orders_Partitioned"
SELECT * FROM sys.partitions WHERE OBJECT_NAME(object_id) = 'Orders_Partitioned';


-- 8) CLEAR A PARTITION, FOR EXAMPLE FG_2022

-- a) get partition number for FG_2022
SELECT p.partition_number
FROM sys.partitions p
INNER JOIN sys.destination_data_spaces dds ON p.partition_number = dds.destination_id
INNER JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
WHERE OBJECT_NAME(p.object_id) = 'Orders_Partitioned' and fg.name = 'FG_2022'; 
-- in my example: PARTITION_NUMER = 1

-- b) truncate this partition 
TRUNCATE TABLE Sales.Orders_Partitioned
WITH (PARTITIONS (1)); -- partition number from previous query


-- 9) DELETE PARTITION FOR EXAMPLE FG_2026

-- a) get partition number for FG_2026
SELECT p.partition_number
FROM sys.partitions p
INNER JOIN sys.destination_data_spaces dds ON p.partition_number = dds.destination_id
INNER JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
WHERE OBJECT_NAME(p.object_id) = 'Orders_Partitioned' and fg.name = 'FG_2026'; 
-- in my example: PARTITION_NUMER = 5

-- b) truncate this partition
TRUNCATE TABLE Sales.Orders_Partitioned
WITH (PARTITIONS (5)); -- partition number from previous query

-- c) connect to neighbour partition for example FG_2025
ALTER PARTITION SCHEME SchemePartitionByYear
NEXT USED FG_2025;

-- c) join two partitions - join current (FG_2025) with deleted partition (FG_2026)
ALTER PARTITION FUNCTION PartitionByYear() MERGE RANGE ('2026-12-31');
-- range of FG_2026 was: >= '2026-01-01', <= '2026-12-31'

-- d) delete data file and file group
ALTER DATABASE AdventureWorks2022 REMOVE FILE P_2026;
ALTER DATABASE AdventureWorks2022 REMOVE FILEGROUP FG_2026;

-- result: FG_2025, FG_2026 -> FG_2025

/* If You do not switch to other partition You will merge FG_2025 into FG_2026.
In other words, if You do this steps:

TRUNCATE TABLE Sales.Orders_Partitioned
WITH (PARTITIONS (5)); -- FG_2026

ALTER PARTITION FUNCTION PartitionByYear() MERGE RANGE ('2025-12-31');

You will truncate FG_2026 partition, but You will not remove this partition. 
You join partition with OrderDate less than '2025-12-31' to FG_2026 partition.

result: FG_2025, FG_2026 -> FG_2026
expected: FG_2025, FG_2026 -> FG_2025
*/

-- 10) INSERT DATA INTO TABLE PARTITION FROM OTHER TABLE

-- a) create table related to partition FG_2022, for example "Table2022"
DROP TABLE IF EXISTS Sales.Table2022;
CREATE TABLE Sales.Table2022(
	OrderID int,
	Orderdate Date,
	Sales int
) ON [FG_2022]; -- relation between [FG_2022] partition of TARGET table (Orders_Partitioned) and SOURCE table (Table2022)

-- get info about partition FG_2022
SELECT p.partition_number, fg.name, p.rows, p.partition_id
FROM sys.partitions p
INNER JOIN sys.destination_data_spaces dds ON p.partition_number = dds.destination_id
INNER JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
WHERE OBJECT_NAME(p.object_id) = 'Orders_Partitioned' and fg.name = 'FG_2022'; 
-- partition FG_2022 (in my example, partition number = 1) is empty (rows = 0)

-- b) insert data into "Table2022"
INSERT INTO Sales.Table2022 VALUES
	(1, '2022-01-31', 100),
	(2, '2022-05-11', 200),
	(3, '2022-11-03', 300);

-- c) add constraint "check" Table2022, because data must be consistent with FG_2022 partition range.
ALTER TABLE Sales.Table2022 ADD CONSTRAINT chk_orderdate_Table2022
	CHECK (OrderDate IS NOT NULL AND OrderDate >= '2022-01-01' AND OrderDate <= '2022-12-31');

-- d) switch data from "Table2022" to partition 1 (FG_2022) of "Orders_Partitioned" table
ALTER TABLE Sales.Table2022 SWITCH TO Sales.Orders_Partitioned PARTITION 1;

SELECT * FROM Sales.Table2022; -- after switch this table is empty
SELECT * FROM Sales.Orders_Partitioned WHERE YEAR(Orderdate) = 2022;


-- 11) EXPORT DATA FROM PARTITION FOR EXAMPLE [FG_2025] TO OTHER TABLE

-- a) create a target table for example "Table2025"
DROP TABLE IF EXISTS Sales.Table2025;
CREATE TABLE Sales.Table2025(
	OrderID int,
	Orderdate Date,
	Sales int
) ON [FG_2025]; -- relation between [FG_2025] partition of SOURCE table (Orders_Partitioned) and TARGET table (Table2025)

-- get info about partition FG_2022
SELECT p.partition_number, fg.name, p.rows, p.partition_id
FROM sys.partitions p
INNER JOIN sys.destination_data_spaces dds ON p.partition_number = dds.destination_id
INNER JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
WHERE OBJECT_NAME(p.object_id) = 'Orders_Partitioned' and fg.name = 'FG_2025'; 
-- partition FG_2025 (in my example, partition number = 4) is empty (rows = 3)

-- b) switch data from partition [FG_2025] to Table2025
ALTER TABLE Sales.Orders_Partitioned switch partition 4 to Sales.Table2025;

SELECT * FROM Sales.Orders_Partitioned WHERE Orderdate like '2025%'; -- partition [FG_2025] is empty
SELECT * FROM Sales.Table2025;


-- CLEANUP
DROP TABLE Sales.Orders_Partitioned;
DROP TABLE Sales.Table2022;
DROP TABLE Sales.Table2025;

DROP PARTITION SCHEME SchemePartitionByYear;
DROP PARTITION FUNCTION PartitionByYear;

ALTER DATABASE AdventureWorks2022 REMOVE FILE P_2022;
ALTER DATABASE AdventureWorks2022 REMOVE FILEGROUP FG_2022;

ALTER DATABASE AdventureWorks2022 REMOVE FILE P_2023;
ALTER DATABASE AdventureWorks2022 REMOVE FILEGROUP FG_2023;

ALTER DATABASE AdventureWorks2022 REMOVE FILE P_2024;
ALTER DATABASE AdventureWorks2022 REMOVE FILEGROUP FG_2024;

ALTER DATABASE AdventureWorks2022 REMOVE FILE P_2025;
ALTER DATABASE AdventureWorks2022 REMOVE FILEGROUP FG_2025;

ALTER DATABASE AdventureWorks2022 REMOVE FILE P_2027;
ALTER DATABASE AdventureWorks2022 REMOVE FILEGROUP FG_2027;

ALTER DATABASE AdventureWorks2022 REMOVE FILE P_2028;
ALTER DATABASE AdventureWorks2022 REMOVE FILEGROUP FG_2028;