-------------------------------------------------------------------------
-- Copyright (c) 2025 Kacper Prusiński

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
-- Script name: PARTITIONS_PROGRAMMABILITY.sql
-------------------------------------------------------------------------

-- PROGRAMMABILITY

USE AdventureWorks2022
GO


CREATE OR ALTER VIEW dbo.v_partitioned_tables AS 
SELECT tp.object_id, tp.table_name, tp.schema_name, 
	CASE
		WHEN tp.function_id IS NULL THEN 'NON PARTITIONED'
		WHEN tp.function_id IS NOT NULL AND OBJECT_ID IS NOT NULL THEN 'PARTITIONED'
		WHEN tp.function_id IS NOT NULL AND OBJECT_ID IS NULL AND tp.data_space_id IS NOT NULL THEN 'EXISTS ONLY FUNCTION AND SCHEME'
		WHEN tp.function_id IS NOT NULL AND OBJECT_ID IS NULL AND tp.data_space_id IS NULL THEN 'EXISTS ONLY FUNCTION'
	END AS is_partitioned,
	pf.name function_name, ps.name scheme_name, tp.part_column_name, tp.type, tp.precision, tp.scale,
	tp.partition_number, prv.boundary_id, COALESCE(prv.value, tp.value1) VALUE, tp.rows, ps.data_space_id, pf.function_id
FROM sys.partition_functions pf
LEFT JOIN sys.partition_schemes ps ON pf.function_id = ps.function_id
LEFT JOIN sys.partition_range_values prv on prv.function_id = ps.function_id
FULL JOIN (
	SELECT t.name table_name, s.name schema_name, c.name part_column_name, ty.name type, c.precision, c.scale, t.object_id,
		ps.function_id, ps.data_space_id, prv.value value1, p.partition_number, p.rows
	FROM sys.tables t
	LEFT JOIN sys.schemas s ON t.schema_id = s.schema_id 
	LEFT JOIN sys.indexes i 
		ON t.object_id = i.object_id AND i.index_id <= 1
	LEFT JOIN sys.index_columns ic
		ON i.object_id = ic.object_id AND i.index_id = ic.index_id AND ic.partition_ordinal = 1
	LEFT JOIN sys.columns c 
		ON ic.column_id = c.column_id AND c.object_id = t.object_id
	LEFT JOIN sys.types ty
		ON c.system_type_id = ty.system_type_id
	LEFT JOIN sys.partitions p 
		ON i.object_id = p.object_id AND i.index_id = p.index_id
	LEFT JOIN sys.partition_schemes ps
		ON i.data_space_id = ps.data_space_id
	LEFT JOIN sys.partition_functions pf 
		ON ps.function_id = pf.function_id 
	LEFT JOIN sys.partition_range_values prv 
		ON prv.function_id = pf.function_id and prv.boundary_id = p.partition_number
) tp ON pf.function_id = tp.function_id AND prv.boundary_id = tp.partition_number;


-- PATTERNS FUNCTION
CREATE OR ALTER FUNCTION dbo.partition_function_name_pattern (
	@table_schema varchar(60),
	@table_name varchar(120)
)
RETURNS nvarchar(300)
AS 
BEGIN
    DECLARE @part_func_tab nvarchar(300);
	
    SET @part_func_tab = TRIM(@table_schema) + '_' +TRIM(@table_name) + '_PF';

    RETURN @part_func_tab; -- output format: [schema]_[table]_PF
END;


CREATE OR ALTER FUNCTION dbo.partition_scheme_name_pattern (
	@table_schema varchar(60),
	@table_name varchar(120),
	@partition_column varchar(50)
)
RETURNS nvarchar(300)
AS 
BEGIN
	DECLARE @part_scheme nvarchar(300);

	SET @part_scheme = TRIM(@table_schema) + '_' +TRIM(@table_name) + '_Scheme_' + @partition_column;	
	
	RETURN @part_scheme; -- output format: "[schema]_[table]_Scheme_[column]"
END;

-- PREPARING NEW PARTITIONED TABLES
CREATE OR ALTER PROCEDURE dbo.print_or_execute
	@print_execute varchar(2),
	@sql nvarchar(MAX)
AS
BEGIN TRY
	DECLARE	@err_msg nvarchar(MAX);

	IF @print_execute = 'P'  
		PRINT @sql;
	ELSE IF @print_execute = 'E' 
		EXEC sp_executesql @sql;
	ELSE IF COALESCE(@print_execute, 'PE') = 'PE'
	BEGIN
		PRINT @sql;
		EXEC sp_executesql @sql;
	END;
	ELSE 
	BEGIN
		SET @err_msg = '@print_execute can get values P - print, E - execute, PE - print and execute';
		RAISERROR(@err_msg, 16, 1);
	END;
END TRY
BEGIN CATCH
	SET @err_msg = ERROR_PROCEDURE() + '; LINE: ' + CAST(ERROR_LINE() AS varchar(4)) + '; MESSAGE: ' + ERROR_MESSAGE();
	RAISERROR(@err_msg, 16, 1);
END CATCH;

CREATE OR ALTER PROCEDURE dbo.create_partition_function
    @table_schema varchar(60),
	@table_name varchar(120),
    @ranges nvarchar(MAX), -- values for partitions, expected delimiter: ',' values without added quotation marks, 'A,B,C...'
    @type_values nvarchar(20), -- int, date, varchar(...), nvarchar etc.
    @range_kind nvarchar(1), -- L - left, R - right,
	@print_execute varchar(2) -- P - only print, E - execute, PE or NULL- print + execute
AS
BEGIN TRY
    SET NOCOUNT ON;

    DECLARE
        @part_func_tab nvarchar(300),
        @sql nvarchar(MAX),
        @err_msg nvarchar(4000);

	IF @table_name IS NULL OR @type_values IS NULL OR @range_kind IS NULL
	BEGIN
		SET @err_msg = 'One or more required parameters are NULL.';
		THROW 50001, @err_msg, 1;
	END;

    IF @range_kind NOT IN ('L', 'R')
    BEGIN
        SET @err_msg = 'Expected value of @range_kind is L (LEFT) or R (RIGHT).';
        THROW 50001, @err_msg, 1;
    END;

    SET @part_func_tab = dbo.partition_function_name_pattern(@table_schema, @table_name);

	IF @ranges IS NULL
		SET @ranges = '';
    ELSE IF LOWER(@type_values) LIKE '%char%' OR LOWER(@type_values) LIKE '%text%' OR LOWER(@type_values) = 'date'
    BEGIN
        SELECT @ranges = STRING_AGG('''' + TRIM(value) + '''', ', ') WITHIN GROUP (ORDER BY TRIM(value))
        FROM STRING_SPLIT(@ranges, ',');
    END
    ELSE IF LOWER(@type_values) LIKE '%int%' OR LOWER(@type_values) LIKE '%num%'
    BEGIN
        SET @ranges = REPLACE(@ranges, ' ', '');
    END
    ELSE
    BEGIN
        SET @err_msg = 'Unsupported @type_values format: ' + @type_values;
        THROW 50001, @err_msg, 1;
    END;

	SET @range_kind = CASE WHEN @range_kind = 'L' THEN 'LEFT' ELSE 'RIGHT' END;

    SET @sql = 'CREATE PARTITION FUNCTION ' + QUOTENAME(@part_func_tab) + ' (' + @type_values + ') ' +
       'AS RANGE ' + CASE WHEN @range_kind = 'L' THEN 'LEFT' ELSE 'RIGHT' END + ' FOR VALUES (' + @ranges + ')';
 
	IF COALESCE(@print_execute, 'PE') NOT IN ('P', 'E', 'PE')
	BEGIN
		SET @err_msg = '@print_execute can get values P - print, E - execute, PE - print and execute';
		RAISERROR(@err_msg, 16, 1);
	END;

	EXEC dbo.print_or_execute @print_execute, @sql;
END TRY
BEGIN CATCH
	SET @err_msg = ERROR_PROCEDURE() + '; LINE: ' + CAST(ERROR_LINE() AS varchar(4)) + '; MESSAGE: ' + ERROR_MESSAGE();
	RAISERROR(@err_msg, 16, 1);
END CATCH;


CREATE OR ALTER PROCEDURE dbo.create_partition_scheme
	@table_schema varchar(60),
	@table_name varchar(120),
	@partition_column varchar(50), -- partition column name
	@print_execute varchar(2) -- P - only print, E - execute, PE or NULL- print + execute
AS
BEGIN TRY
	DECLARE 
		@sql nvarchar(MAX),
		@err_msg nvarchar(MAX),
		@partition_function nvarchar(300),
		@partition_scheme nvarchar(300);

	IF @table_name IS NULL OR @partition_column IS NULL
		THROW 50001, 'Parameter @table_name or @partition_name can not be null', 1; 

	SET @table_name = REPLACE(@table_name, ' ', '');
	SET @table_schema = REPLACE(@table_schema, ' ', '');
	SET @partition_column = REPLACE(@partition_column, ' ', '');

	SET @partition_function = dbo.partition_function_name_pattern(@table_schema, @table_name);
	SET @partition_scheme = dbo.partition_scheme_name_pattern(@table_schema, @table_name, @partition_column);

	SET @sql = 'CREATE PARTITION SCHEME ' + @partition_scheme;
	SET @sql += ' AS PARTITION ' + @partition_function + ' ALL TO ([PRIMARY])';

	IF COALESCE(@print_execute, 'PE') NOT IN ('P', 'E', 'PE')
	BEGIN
		SET @err_msg = '@print_execute can get values P - print, E - execute, PE - print and execute';
		RAISERROR(@err_msg, 16, 1);
	END;

	EXEC dbo.print_or_execute @print_execute, @sql;
END TRY
BEGIN CATCH
	SET @err_msg = ERROR_PROCEDURE() + '; LINE: ' + CAST(ERROR_LINE() AS varchar(4)) + '; MESSAGE: ' + ERROR_MESSAGE();
	RAISERROR(@err_msg, 16, 1);
END CATCH;


CREATE OR ALTER PROCEDURE dbo.prepare_new_partition_table
	@table_schema varchar(60),
	@table_name varchar(120),
	@partition_column varchar(50), -- partition column name
    @ranges nvarchar(MAX), -- values for partitions, expected delimiter: ',' values without added quotation marks, 'A,B,C...'
    @type_values varchar(20), -- int, date, varchar(...), nvarchar etc.
    @range_kind varchar(1), -- L - left, R - right
	@print_execute varchar(2) -- P - only print, E - execute, PE or NULL- print + execute
AS
BEGIN TRY
	DECLARE 
		@err_msg nvarchar(MAX)

	SET @table_schema = COALESCE(@table_schema, 'dbo');
		
	IF SCHEMA_ID(@table_schema) IS NULL
	BEGIN
		SET @err_msg = 'Schema ' + @table_schema + ' does not exists';
		RAISERROR(@err_msg, 16, 1);
	END;

	IF @table_name IS NULL OR @partition_column IS NULL OR @type_values IS NULL OR @range_kind IS NULL
		RAISERROR('One of parameter: @table_name, @partition_column, @type_values, @range_kind IS NULL', 16, 1);

	EXEC dbo.create_partition_function @table_schema, @table_name, @ranges, @type_values, @range_kind, @print_execute;
	EXEC dbo.create_partition_scheme @table_schema, @table_name, @partition_column, @print_execute;

END TRY
BEGIN CATCH
	CLOSE value_cur;
	DEALLOCATE value_cur;

    SET @err_msg = ERROR_PROCEDURE() + '; LINE: ' + CAST(ERROR_LINE() AS varchar(4)) + '; MESSAGE: ' + ERROR_MESSAGE();
	RAISERROR(@err_msg, 16, 1);
END CATCH;


CREATE OR ALTER FUNCTION dbo.is_table_partitioned (
	@table_schema varchar(60),
	@table_name varchar(120)
)
RETURNS INT
BEGIN
	SET @table_name = REPLACE(@table_name, ' ', '');
	SET @table_schema = REPLACE(@table_schema, ' ', '');

	DECLARE 
		@is_partitioned int = 0,
		@table varchar(181) = COALESCE(UPPER(@table_schema), 'dbo') + '.' + COALESCE(UPPER(@table_name), 'TABLE');
	
	-- results:
	-- -1 - table not exists
	-- 0 - table is not partitioned or does not exists
	-- 1 - table is partitioned

	 SET @is_partitioned = COALESCE((
		SELECT TOP 1
			CASE is_partitioned
				WHEN 'NON PARTITIONED' THEN 0
				WHEN 'PARTITIONED' THEN 1
			END
		FROM dbo.v_partitioned_tables
		WHERE object_id = OBJECT_ID(@table)
	), -1);

	RETURN @is_partitioned;
END;


-- PREPARING TABLE
CREATE OR ALTER PROCEDURE dbo.add_partition
	@partition_function varchar(300),
	@partition_scheme varchar(300),
	@value nvarchar(100),
	@print_execute varchar(2)
AS
BEGIN TRY
	DECLARE 
		@sql nvarchar(MAX),
		@err_msg nvarchar(MAX);

	SET @sql = 'ALTER PARTITION SCHEME ' + @partition_scheme +  ' NEXT USED [PRIMARY]';
	EXEC dbo.print_or_execute @print_execute, @sql;

	SET @sql = 'ALTER PARTITION FUNCTION ' + @partition_function + '() SPLIT RANGE (' + @value + ')';
	EXEC dbo.print_or_execute @print_execute, @sql;
END TRY
BEGIN CATCH
	SET @err_msg = ERROR_PROCEDURE() + '; LINE: ' + CAST(ERROR_LINE() AS varchar(4)) + '; MESSAGE: ' + ERROR_MESSAGE();
	RAISERROR(@err_msg, 16, 1);
END CATCH;


CREATE OR ALTER PROCEDURE dbo.prepare_table
	@table_schema varchar(60),
	@table_name	nvarchar(120),
	@partition_column nvarchar(50),
	@value nvarchar(100),
	@delete_condition nvarchar(MAX),
	@to_remove char(1),
	@print_execute char(2)
AS
BEGIN TRY
	DECLARE
		@sql nvarchar(MAX),
		@partition_function nvarchar(300),
		@partition_scheme nvarchar(300),
		@partition_number int,
		@count bigint,
		@column varchar(50),
		@type varchar(20),
		@sql_variant_cast nvarchar(MAX),
		@v_value varchar(100),
		@err_msg nvarchar(MAX),
		@table nvarchar(MAX);

	SET @table_schema = COALESCE(@table_schema, 'dbo');
	SET @table = UPPER(@table_schema + '.' + @table_name);
	SET @to_remove = COALESCE(@to_remove, 'N');

	IF @to_remove NOT IN ('Y', 'N')
		RAISERROR('Parameter @to_remove get values ''Y'' or ''N''', 16, 1);

	SELECT TOP 1 @partition_function = function_name, @partition_scheme = scheme_name, @type = type,
		@column = part_column_name
	FROM dbo.v_partitioned_tables WHERE OBJECT_ID = OBJECT_ID(@table);

	SET @v_value = CASE 
		WHEN @type LIKE '%char%' OR @type LIKE '%text%' OR @type LIKE '%date%' THEN REPLACE('''VALUE''', 'VALUE', TRIM(@value))
		ELSE @value
	END;

	SET @sql_variant_cast = 'CAST(CAST(' + @v_value + ' AS ' + @type + ') AS sql_variant)';

	IF @partition_column IS NULL AND @value IS NULL AND @delete_condition IS NULL
	BEGIN
		SET @sql = 'TRUNCATE TABLE ' + @table;
		EXEC dbo.print_or_execute @print_execute, @sql;
	END
	ELSE IF (@delete_condition IS NOT NULL AND @partition_column IS NULL AND @value IS NULL)
	BEGIN
		SET @sql = 'DELETE FROM ' + @table + ' WHERE ' + @delete_condition + ' OPTION (MAXDOP 10)';		
		EXEC dbo.print_or_execute @print_execute, @sql;
	END
	ELSE IF (@delete_condition IS NULL AND @partition_column IS NOT NULL AND @value IS NOT NULL)
	BEGIN
		IF @partition_function IS NOT NULL
		BEGIN
			SET @sql = 'SELECT @NUMBER = partition_number, @ROWS = rows FROM dbo.v_partitioned_tables' +
				' WHERE object_id = ' + CAST(OBJECT_ID(@table) as varchar(20)) +
				' AND VALUE = ' + @sql_variant_cast + 'GROUP BY partition_number, rows';

			EXEC sp_executesql @sql, N'@NUMBER int OUTPUT, @ROWS bigint OUTPUT', 
				@NUMBER = @partition_number OUTPUT, @ROWS = @count OUTPUT;

			IF @partition_number IS NULL
				EXEC dbo.add_partition @partition_function, @partition_scheme, @v_value, @print_execute;
			ELSE
			BEGIN
				IF @count > 0
				BEGIN
					SET @sql = 'TRUNCATE TABLE ' + @table + ' WITH(PARTITIONS (' + CAST(@partition_number AS varchar(4)) + '))';
					EXEC dbo.print_or_execute @print_execute, @sql;
				END;

				IF @to_remove = 'Y'
				BEGIN
					SET @sql = 'ALTER PARTITION SCHEME ' + @partition_scheme +  ' NEXT USED [PRIMARY]';
					EXEC dbo.print_or_execute @print_execute, @sql;

					SET @sql = 'ALTER PARTITION FUNCTION ' + @partition_function + '() MERGE RANGE (' + @v_value + ')';
					EXEC dbo.print_or_execute @print_execute, @sql;
				END;
			END;
		END
		ELSE
		BEGIN
			SET @sql = 'DELETE FROM ' + @table + ' WHERE $delete_condition OPTION (MAXDOP 10)';		
			SET @sql = REPLACE(@sql, '$delete_condition', COALESCE(@partition_column + ' = ' + @v_value, @delete_condition));
			EXEC dbo.print_or_execute @print_execute, @sql;
		END;
	END
	ELSE IF @delete_condition IS NOT NULL
	BEGIN
		SET @sql = 'DELETE FROM ' + @table + ' WHERE $column = $value AND $delete_condition OPTION (MAXDOP 10)';		
		SET @sql = REPLACE(REPLACE(REPLACE(@sql, '$column', @partition_column), '$value', @v_value), '$delete_condition', @delete_condition);
		EXEC dbo.print_or_execute @print_execute, @sql; 
	END;
END TRY
BEGIN CATCH
	SET @err_msg = ERROR_PROCEDURE() + '; LINE: ' + CAST(ERROR_LINE() AS varchar(4)) + '; MESSAGE: ' + ERROR_MESSAGE();
	RAISERROR(@err_msg, 16, 1);
END CATCH;


-- CLEANING
CREATE OR ALTER PROCEDURE dbo.drop_table
	@table_schema varchar(60),
	@table_name varchar(120),
	@print_execute varchar(2)
AS
BEGIN TRY
	DECLARE
		@table varchar(181),
		@partition_scheme varchar(300),
		@partition_function varchar(300),
		@err_msg nvarchar(2000),
		@sql nvarchar(MAX);

	SET @table = TRIM(@table_schema) + '.' + TRIM(@table_name);

	IF OBJECT_ID(@table) IS NULL
	BEGIN
		SET @err_msg = ERROR_PROCEDURE() + '; LINE: ' + cast(ERROR_LINE() AS VARCHAR(4)) + '; ' + ERROR_MESSAGE();
		RAISERROR(@err_msg, 16, 1);
	END;
	
	SELECT TOP 1 @partition_scheme = scheme_name, @partition_function = function_name
	FROM v_partitioned_tables
	WHERE object_id = OBJECT_ID(@table);

	SET @sql = 'DROP TABLE ' + @table;
	EXEC dbo.print_or_execute @print_execute, @sql;

	IF @partition_scheme IS NOT NULL
	BEGIN
		SET @sql = 'DROP PARTITION SCHEME ' + @partition_scheme;
		EXEC dbo.print_or_execute @print_execute, @sql;
	END;

	IF @partition_function IS NOT NULL
	BEGIN
		SET @sql = 'DROP PARTITION FUNCTION ' + @partition_function;
		EXEC dbo.print_or_execute @print_execute, @sql;
	END;
END TRY
BEGIN CATCH
	SET @err_msg = ERROR_PROCEDURE() + '; LINE: ' + cast(ERROR_LINE() AS VARCHAR(4)) + '; ' + ERROR_MESSAGE();
	RAISERROR(@err_msg, 16, 1);
END CATCH;


-- TESTING

-- date type of partition column
EXEC dbo.prepare_new_partition_table @table_schema = 'Sales', @table_name = 'Order_Partitioned', @partition_column = 'OrderDate',
	@ranges = '2023-12-31,2024-12-31,2025-12-31,2026-12-31', @type_values = 'Date', @range_kind = 'L', @print_execute = 'PE';

-- CREATE TABLE
CREATE TABLE Sales.Order_Partitioned(
	OrderID int,
	Orderdate Date,
	Sales int
) ON Sales_Order_Partitioned_Scheme_OrderDate(OrderDate); 

-- Sales_Order_Partitioned_Scheme_OrderDate - partition scheme
-- OrderDate - partition column


INSERT INTO Sales.Order_Partitioned VALUES
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

SELECT * FROM v_partitioned_tables WHERE object_id = OBJECT_ID('Sales.Order_Partitioned');

-- add partition for value
EXEC dbo.add_partition @partition_function = 'Sales_Order_Partitioned_PF', 
	@partition_scheme = 'Sales_Order_Partitioned_Scheme_OrderDate', @value = '''2022-12-31''', @print_execute = 'P'; -- default PE

INSERT INTO Sales.Order_Partitioned VALUES
	(13, '2022-05-15', 300), 
	(14, '2022-07-20', 200), 
	(15, '2022-07-20', 107);

SELECT sod.*, $partition.Sales_Order_Partitioned_PF(OrderDate) number FROM Sales.Order_Partitioned sod;

-- add new partition
EXEC dbo.prepare_table @table_schema = 'Sales', @table_name = 'Order_Partitioned', @partition_column = 'OrderDate', 
	@value = '2027-12-31', @delete_condition = NULL, @to_remove = NULL, @print_execute = NULL; 

-- add new partition
EXEC dbo.prepare_table @table_schema = 'Sales', @table_name = 'Order_Partitioned', @partition_column = 'OrderDate', 
	@value = '2028-12-31', @delete_condition = NULL, @to_remove = NULL, @print_execute = NULL;

INSERT INTO Sales.Order_Partitioned VALUES
	(16, '2027-08-21', 200), 
	(17, '2027-08-23', 200),
	(18, '2027-08-23', 200);

INSERT INTO Sales.Order_Partitioned VALUES
	(19, '2028-08-21', 200),
	(20, '2028-10-12', 200),
	(21, '2028-03-01', 200);

-- remove partition with OrderDate < '2025-12-31'
EXEC dbo.prepare_table @table_schema = 'Sales', @table_name = 'Order_Partitioned', @partition_column = 'OrderDate', 
	@value = '2025-12-31', @delete_condition = NULL, @to_remove = 'Y', @print_execute = NULL;

-- truncate partition with OrderDate < '2025-12-31'
EXEC dbo.prepare_table @table_schema = 'Sales', @table_name = 'Order_Partitioned', @partition_column = 'OrderDate', 
	@value = '2026-12-31', @delete_condition = NULL, @to_remove = 'N', @print_execute = NULL;

SELECT sod.*, $partition.Sales_Order_Partitioned_PF(OrderDate) number FROM Sales.Order_Partitioned sod;
SELECT * FROM v_partitioned_tables WHERE object_id = OBJECT_ID('Sales.Order_Partitioned');

-- delete all orders before 2024-12-31 with order_id > 10
EXEC dbo.prepare_table @table_schema = 'Sales', @table_name ='Order_Partitioned',
	@partition_column = NULL, @value = NULL, @delete_condition = 'OrderDate < convert(date, ''2024-12-31'') and OrderID > 10', 
	@to_remove = NULL, @print_execute = 'PE';

-- truncate table Sales.Order_Partitioned
EXEC dbo.prepare_table @table_schema = 'Sales', @table_name ='Order_Partitioned',
	@partition_column = NULL, @value = NULL, @delete_condition = NULL, @to_remove = NULL, @print_execute = 'PE';

-- drop table, partition scheme and partition function
EXEC dbo.drop_table 'Sales', 'Order_Partitioned', 'PE';



-- int type of partition column
EXEC dbo.prepare_new_partition_table @table_schema = 'Sales', @table_name = 'Order_Partitioned', @partition_column = 'OrderYear',
	@ranges = '2023,2024,2025,2026', @type_values = 'int', @range_kind = 'L', @print_execute = 'PE';

-- CREATE TABLE
CREATE TABLE Sales.Order_Partitioned(
	OrderID int,
	Orderdate Date,
	Sales int,
	OrderYear int,
) ON Sales_Order_Partitioned_Scheme_OrderYear(OrderYear); 

-- Sales_Order_Partitioned_Scheme_OrderYear - partition scheme
-- OrderYear - partition column


INSERT INTO Sales.Order_Partitioned VALUES
	(1,  '2023-05-15', 100, 2023),
	(2,  '2024-07-20', 100, 2024),
	(3,  '2025-07-20', 60,  2025),
	(4,  '2026-07-21', 200, 2026), 
	(5,  '2023-07-15', 100, 2023),
	(6,  '2024-11-19', 100, 2024),
	(7,  '2025-12-20', 60,  2025), 
	(8,  '2026-01-21', 200, 2026), 
	(9,  '2023-02-15', 100, 2023), 
	(10, '2024-04-20', 100, 2024), 
	(11, '2025-09-13', 60,  2025),
	(12, '2026-07-04', 200, 2026); 

SELECT * FROM v_partitioned_tables WHERE object_id = OBJECT_ID('Sales.Order_Partitioned');

-- add partition for value
EXEC dbo.add_partition @partition_function = 'Sales_Order_Partitioned_PF', 
	@partition_scheme = 'Sales_Order_Partitioned_Scheme_OrderYear', @value = '2022', @print_execute = 'PE'; -- default PE

INSERT INTO Sales.Order_Partitioned VALUES
	(13, '2022-05-15', 300, 2022), 
	(14, '2022-07-20', 200, 2022), 
	(15, '2022-07-20', 107, 2022);

SELECT sod.*, $partition.Sales_Order_Partitioned_PF(OrderYear) number FROM Sales.Order_Partitioned sod;

-- add new partition
EXEC dbo.prepare_table @table_schema = 'Sales', @table_name = 'Order_Partitioned', @partition_column = 'OrderYear', 
	@value = '2027', @delete_condition = NULL, @to_remove = NULL, @print_execute = NULL; 

-- add new partition
EXEC dbo.prepare_table @table_schema = 'Sales', @table_name = 'Order_Partitioned', @partition_column = 'OrderYear', 
	@value = '2028', @delete_condition = NULL, @to_remove = NULL, @print_execute = NULL;

INSERT INTO Sales.Order_Partitioned VALUES
	(16, '2027-08-21', 200, 2027), 
	(17, '2027-08-23', 200, 2027),
	(18, '2027-08-23', 200, 2027);

INSERT INTO Sales.Order_Partitioned VALUES
	(19, '2028-08-21', 200, 2028),
	(20, '2028-10-12', 200, 2028),
	(21, '2028-03-01', 200, 2028);

-- remove partition with OrderDate < '2025-12-31'
EXEC dbo.prepare_table @table_schema = 'Sales', @table_name = 'Order_Partitioned', @partition_column = 'OrderYear', 
	@value = '2025', @delete_condition = NULL, @to_remove = 'Y', @print_execute = NULL;

-- truncate partition with OrderDate < '2026-12-31'
EXEC dbo.prepare_table @table_schema = 'Sales', @table_name = 'Order_Partitioned', @partition_column = 'OrderYear', 
	@value = '2026', @delete_condition = NULL, @to_remove = 'N', @print_execute = NULL;

SELECT sod.*, $partition.Sales_Order_Partitioned_PF(OrderYear) number FROM Sales.Order_Partitioned sod;
SELECT * FROM v_partitioned_tables WHERE object_id = OBJECT_ID('Sales.Order_Partitioned');

-- delete all orders before 2024-12-31 with order_id > 10
EXEC dbo.prepare_table @table_schema = 'Sales', @table_name ='Order_Partitioned',
	@partition_column = NULL, @value = NULL, @delete_condition = 'OrderYear <= 2024 and OrderID > 10', 
	@to_remove = NULL, @print_execute = 'PE';

-- truncate table Sales.Order_Partitioned
EXEC dbo.prepare_table @table_schema = 'Sales', @table_name ='Order_Partitioned',
	@partition_column = NULL, @value = NULL, @delete_condition = NULL, @to_remove = NULL, @print_execute = 'PE';

-- drop table, partition scheme and partition function
EXEC dbo.drop_table 'Sales', 'Order_Partitioned', 'PE';



-- non partitioned table
CREATE TABLE Sales.Order_Non_Partitioned(
	OrderID int,
	Orderdate Date,
	Sales int
); 


INSERT INTO Sales.Order_Non_Partitioned VALUES
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


-- DELETE FROM SALES.ORDER_NON_PARTITIONED WHERE OrderId = 1 OPTION (MAXDOP 10)
EXEC dbo.prepare_table @table_schema = 'Sales', @table_name = 'Order_Non_Partitioned', @partition_column = 'OrderId', @value = '1',
	@delete_condition = NULL, @to_remove = NULL, @print_execute = 'PE';

-- DELETE FROM SALES.ORDER_NON_PARTITIONED WHERE YEAR(OrderDate) = 2023 OPTION (MAXDOP 10)
EXEC dbo.prepare_table @table_schema = 'Sales', @table_name = 'Order_Non_Partitioned', @partition_column = NULL, @value = NULL,
	@delete_condition = 'YEAR(OrderDate) = 2023', @to_remove = NULL, @print_execute = 'PE';

-- TRUNCATE TABLE SALES.ORDER_NON_PARTITIONED
EXEC dbo.prepare_table @table_schema = 'Sales', @table_name ='Order_Non_Partitioned',
	@partition_column = NULL, @value = NULL, @delete_condition = NULL, @to_remove = NULL, @print_execute = 'PE';

-- drop table, partition scheme and partition function
EXEC dbo.drop_table 'Sales', 'Order_Non_Partitioned', 'PE';
