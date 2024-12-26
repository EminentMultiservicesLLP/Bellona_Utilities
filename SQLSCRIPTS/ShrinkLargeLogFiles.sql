CREATE PROCEDURE ShrinkLargeLogFiles
AS
BEGIN
    SET NOCOUNT ON;

    -- Declare a cursor to iterate through databases with log file sizes > 250 MB
    DECLARE @DatabaseName NVARCHAR(255);
    DECLARE @LogicalName NVARCHAR(255);
    DECLARE @ShrinkCommand NVARCHAR(MAX);

    -- Temporary table to store database and log file information
    CREATE TABLE #LogFilesToShrink (
        DatabaseName NVARCHAR(255),
        LogicalName NVARCHAR(255),
        FileSizeMB FLOAT
    );

    -- Populate the temporary table with log files greater than 250 MB
    INSERT INTO #LogFilesToShrink (DatabaseName, LogicalName, FileSizeMB)
    SELECT 
        DB_NAME(database_id) AS DatabaseName,
        name AS LogicalName,
        size / 128.0 AS FileSizeMB
    FROM 
        sys.master_files
    WHERE 
        type_desc = 'LOG' -- Only log files
        AND size / 128.0 > 250; -- Size in MB

    -- Cursor to iterate over log files
    DECLARE log_cursor CURSOR FOR
    SELECT DatabaseName, LogicalName
    FROM #LogFilesToShrink;

    OPEN log_cursor;

    FETCH NEXT FROM log_cursor INTO @DatabaseName, @LogicalName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Build the shrink command dynamically
        SET @ShrinkCommand = 
            'USE [' + @DatabaseName + ']; DBCC SHRINKFILE (''' + @LogicalName + ''', 250);';

        -- Execute the shrink command
        PRINT 'Executing: ' + @ShrinkCommand; -- For debugging
        EXEC sp_executesql @ShrinkCommand;

        FETCH NEXT FROM log_cursor INTO @DatabaseName, @LogicalName;
    END;

    CLOSE log_cursor;
    DEALLOCATE log_cursor;

    -- Clean up
    DROP TABLE #LogFilesToShrink;

    PRINT 'Log file shrink operation completed.';
END;
GO
