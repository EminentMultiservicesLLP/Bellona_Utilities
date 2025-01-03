USE [BELLONA_dev]
GO
/****** Object:  StoredProcedure [dbo].[ReBuild_RistaTables]    Script Date: 25-12-2024 00:11:56 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
exec [dbo].[ReBuild_RistaTables]
ALTER proc [dbo].[ReBuild_RistaTables] as
begin
	/** for MUM (ISHAARA PALLEDIUM) we get different Account names for all billed and hence need to fix it */
	UPDATE Rista_SaleItems 
	SET AccountName = 'Food Sale' Where AccountName ='Food'  or AccountName = 'Foods';

	UPDATE Rista_SaleItems 
	SET AccountName = 'Beverage Sale' Where AccountName ='Beverage' or AccountName = 'Beverages';

	UPDATE Rista_SaleItems 
	SET AccountName = 'Liquor Sale' Where AccountName ='Liquor' or AccountName = 'Liquors';

	UPDATE Rista_SaleItems 
	SET AccountName = 'Tobacco Sale' Where AccountName ='Tobacco' or AccountName ='Tobaccos';

	/*   
	ALTER INDEX ALL ON Rista_SaleInvoices REBUILD;
	ALTER INDEX ALL ON Rista_SaleItems REBUILD;
	ALTER INDEX ALL ON Rista_SalePayments REBUILD;
	ALTER INDEX ALL ON Rista_SaleSourceInfo REBUILD;
	ALTER INDEX ALL ON Rista_SaleInvoiceItems REBUILD;
	ALTER INDEX ALL ON Rista_SaleDelivery REBUILD;

	ALTER INDEX ALL ON Rista_SaleInvoices REORGANIZE;
	ALTER INDEX ALL ON Rista_SaleItems REORGANIZE;
	ALTER INDEX ALL ON Rista_SalePayments REORGANIZE;
	ALTER INDEX ALL ON Rista_SaleSourceInfo REORGANIZE;
	ALTER INDEX ALL ON Rista_SaleInvoiceItems REORGANIZE;
	ALTER INDEX ALL ON Rista_SaleDelivery REORGANIZE;
	*/

	 DECLARE @tableName NVARCHAR(255), @sql NVARCHAR(MAX), @dayOfYear INT;

    -- Get the current day of the year
    SET @dayOfYear = DATEPART(DAYOFYEAR, GETDATE());

    DECLARE table_cursor CURSOR FOR
    SELECT name
    FROM sys.tables
    WHERE is_ms_shipped = 0;  -- Only user tables, excluding system tables

    OPEN table_cursor;
    FETCH NEXT FROM table_cursor INTO @tableName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            -- If today is every 3rd day (dayOfYear % 3 = 0), rebuild indexes, otherwise reorganize
            IF @dayOfYear % 3 = 0
            BEGIN
                -- Rebuild indexes every 3rd day
                SET @sql = 'ALTER INDEX ALL ON [' + @tableName + '] REBUILD;';
                EXEC sp_executesql @sql;
                PRINT 'Successfully rebuilt indexes for table: ' + @tableName;
            END
            ELSE
            BEGIN
                -- Reorganize indexes on other days
                SET @sql = 'ALTER INDEX ALL ON [' + @tableName + '] REORGANIZE;';
                EXEC sp_executesql @sql;
                PRINT 'Successfully reorganized indexes for table: ' + @tableName;
            END
        END TRY
        BEGIN CATCH
            -- Log the error information
            PRINT 'Error processing indexes for table: ' + @tableName;
            PRINT 'Error Message: ' + ERROR_MESSAGE();
        END CATCH;

        -- Fetch the next table name
        FETCH NEXT FROM table_cursor INTO @tableName;
    END

    CLOSE table_cursor;
    DEALLOCATE table_cursor;

end
