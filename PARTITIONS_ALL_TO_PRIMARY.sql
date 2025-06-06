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
-- Script name: PARTITIONS_ALL_TO_PRIMARY.sql
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


-- 2) CREATE PARTITION SCHEME BASED ON FILE GROUP "PRIMARY"

CREATE PARTITION SCHEME SchemePartitionByYear
AS PARTITION PartitionByYear
ALL TO ([PRIMARY]);


-- 3) CREATE PARTITIONED TABLE BASED ON PARTITION SCHEME
CREATE TABLE Sales.Orders_Partitioned(
	OrderID int,
	Orderdate Date,
	Sales int
) ON SchemePartitionByYear(OrderDate);

/*
SchemePartitionByYear - partition scheme
OrderDate - column by which we partition this table
*/

-- 4) INSERTING DATA
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


-- 7) ADD NEW FILE GROUPS AND PARTITIONS TO EXISTING TABLE (ALL TO PRIMARY)

-- a) alter scheme - set file group "PRIMARY" 
ALTER PARTITION SCHEME SchemePartitionByYear
NEXT USED [PRIMARY];

-- b) alter partition function - split new range from neighbour partition range
-- split '2022-12-31' from '2023-12-31'
ALTER PARTITION FUNCTION PartitionByYear()
SPLIT RANGE ('2022-12-31');

-- c) insert data
INSERT INTO Sales.Orders_Partitioned VALUES
	(13, '2022-05-15', 300), 
	(14, '2022-07-20', 200), 
	(15, '2022-07-20', 107);


-- analogical for partitions for 2027 and 2028 Year of OrderDate 
-- 2027

ALTER PARTITION SCHEME SchemePartitionByYear
NEXT USED [PRIMARY];

ALTER PARTITION FUNCTION PartitionByYear()
SPLIT RANGE ('2027-12-31');

INSERT INTO Sales.Orders_Partitioned VALUES
	(16, '2027-08-21', 200), 
	(17, '2027-08-23', 200),
	(18, '2027-08-23', 200);


-- 2028
ALTER PARTITION SCHEME SchemePartitionByYear
NEXT USED [PRIMARY];

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


-- 8) CLEAR A PARTITION, FOR EXAMPLE 2022

-- a) get partition number for YEAR(Orderdate) = 2022
SELECT TOP 1 $Partition.PartitionByYear(Orderdate) as PartitionNumber 
FROM Sales.Orders_Partitioned op 
WHERE YEAR(Orderdate) = 2022; -- in my example: PARTITION_NUMER = 1

-- b) truncate this partition 
TRUNCATE TABLE Sales.Orders_Partitioned
WITH (PARTITIONS (1)); -- partition number from previous query


-- 9) DELETE PARTITION FOR EXAMPLE 2026

-- a) get partition number for FG_2026
SELECT TOP 1 $Partition.PartitionByYear(Orderdate) as PartitionNumber 
FROM Sales.Orders_Partitioned op 
WHERE YEAR(Orderdate) = 2026;
-- in my example: PARTITION_NUMER = 5

-- b) truncate this partition
TRUNCATE TABLE Sales.Orders_Partitioned
WITH (PARTITIONS (5)); -- partition number from previous query

-- c) merge range
ALTER PARTITION FUNCTION PartitionByYear() MERGE RANGE ('2026-12-31');

/* 
	-- test for merging

	ALTER PARTITION SCHEME SchemePartitionByYear
	NEXT USED [PRIMARY];

	ALTER PARTITION FUNCTION PartitionByYear()
	SPLIT RANGE ('2026-12-31');

	BEGIN TRANSACTION
		INSERT INTO Sales.Orders_Partitioned VALUES
			(100, '2026-08-21', 200),
			(101, '2026-10-12', 200);

		SELECT op.*, $Partition.PartitionByYear(Orderdate) as PartitionNumber 
		FROM Sales.Orders_Partitioned op;
	ROLLBACK;

	ALTER PARTITION FUNCTION PartitionByYear() MERGE RANGE ('2026-12-31');
*/


-- 10) INSERT DATA INTO TABLE PARTITION FROM OTHER TABLE

-- a) create table related to partition FG_2022, for example "Table2022"
DROP TABLE IF EXISTS Sales.Table2022;
CREATE TABLE Sales.Table2022(
	OrderID int,
	Orderdate Date,
	Sales int
) ON [PRIMARY]; -- relation between [PRIMARY] partition of TARGET table (Orders_Partitioned) and SOURCE table (Table2022)

-- get info about 'Sales.Orders_Partitioned' table partition for year 2022
SELECT 
    t.name AS table_name,
	t.object_id,
    i.index_id,
	i.data_space_id,
    ps.name AS partition_scheme,
	ps.data_space_id,
    pf.name AS partition_function,
    p.partition_number - 1 as partition_number, -- PRIMARY = 1
    prv.value AS boundary_value,
	prv.boundary_id
FROM sys.tables t
INNER JOIN sys.indexes i 
    ON t.object_id = i.object_id AND i.index_id <= 1 -- clustered or heap only
INNER JOIN sys.partitions p 
    ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.partition_schemes ps 
    ON i.data_space_id = ps.data_space_id
INNER JOIN sys.partition_functions pf 
    ON ps.function_id = pf.function_id AND ps.function_id = pf.function_id
LEFT JOIN sys.partition_range_values prv 
    ON pf.function_id = prv.function_id 
    AND prv.boundary_id + 1 = p.partition_number
WHERE t.object_id  = OBJECT_ID('Sales.Orders_Partitioned') AND YEAR(try_convert(date, prv.value)) = 2022
ORDER BY boundary_value;

-- b) insert data into "Table2022"
INSERT INTO Sales.Table2022 VALUES
	(1, '2022-01-31', 100),
	(2, '2022-05-11', 200),
	(3, '2022-11-03', 300);

-- c) add constraint "check" Table2022, because data must be consistent with 2022 partition range.
ALTER TABLE Sales.Table2022 ADD CONSTRAINT chk_orderdate_Table2022
	CHECK (OrderDate IS NOT NULL AND OrderDate >= '2022-01-01' AND OrderDate <= '2022-12-31');

-- d) switch data from "Table2022" to partition 1 (2022) of "Orders_Partitioned" table
ALTER TABLE Sales.Table2022 SWITCH TO Sales.Orders_Partitioned PARTITION 1;

SELECT * FROM Sales.Table2022; -- after switch this table is empty
SELECT * FROM Sales.Orders_Partitioned WHERE YEAR(Orderdate) = 2022;


-- 11) EXPORT DATA FROM PARTITION FOR YEAR(ORDERDATE) = 2025 TO OTHER TABLE

-- a) create a target table for example "Table2025"
DROP TABLE IF EXISTS Sales.Table2025;
CREATE TABLE Sales.Table2025(
	OrderID int,
	Orderdate Date,
	Sales int
) ON [PRIMARY]; -- relation between [PRIMARY] partition of SOURCE table (Orders_Partitioned) and TARGET table (Table2025)

-- get info about 'Sales.Orders_Partitioned' table partition for year 2025
SELECT 
    t.name AS table_name,
	t.object_id,
    i.index_id,
	i.data_space_id,
    ps.name AS partition_scheme,
	ps.data_space_id,
    pf.name AS partition_function,
    p.partition_number - 1 as partition_number, -- PRIMARY = 1
    prv.value AS boundary_value,
	prv.boundary_id
FROM sys.tables t
INNER JOIN sys.indexes i 
    ON t.object_id = i.object_id AND i.index_id <= 1 -- clustered or heap only
INNER JOIN sys.partitions p 
    ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.partition_schemes ps 
    ON i.data_space_id = ps.data_space_id
INNER JOIN sys.partition_functions pf 
    ON ps.function_id = pf.function_id AND ps.function_id = pf.function_id
LEFT JOIN sys.partition_range_values prv 
    ON pf.function_id = prv.function_id 
    AND prv.boundary_id + 1 = p.partition_number
WHERE t.object_id  = OBJECT_ID('Sales.Orders_Partitioned') AND YEAR(try_convert(date, prv.value)) = 2025
ORDER BY boundary_value;
-- partition with Year 2025 (in my example, partition number = 4) is empty (rows = 3)

-- b) switch data from partition 2025 to Table2025
ALTER TABLE Sales.Orders_Partitioned switch partition 4 to Sales.Table2025;

SELECT * FROM Sales.Orders_Partitioned WHERE Orderdate like '2025%'; -- partition with YEAR(OrderDate) = 2025 is empty
SELECT * FROM Sales.Table2025;


-- CLEANUP
DROP TABLE Sales.Orders_Partitioned;
DROP TABLE Sales.Table2022;
DROP TABLE Sales.Table2025;

DROP PARTITION SCHEME SchemePartitionByYear;
DROP PARTITION FUNCTION PartitionByYear;