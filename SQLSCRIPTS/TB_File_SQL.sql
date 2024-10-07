/***
drop table TB_TrialBalance;
drop table TB_Particulars;
drop table TB_MISHead;
drop table TB_error_log;
drop table TB_FILE_MONTH_LINK;
drop table TB_error_log_archieve;
**/
go


CREATE TABLE TB_MISHead (
    head_id INT PRIMARY KEY IDENTITY(1,1),
    head_name VARCHAR(100) UNIQUE NOT NULL,
	nature varchar(20)
);
go
CREATE TABLE TB_Particulars (
	Id INT PRIMARY KEY IDENTITY(1,1),
    code VARCHAR(20),
	particulars VARCHAR(100) NOT NULL
);
go

CREATE  TABLE TB_FILE_MONTH_LINK (
	Id INT PRIMARY KEY IDENTITY(1,1),
    TBFileName	NVARCHAR(max),
	TBMonth		INT,
	TBYear		INT,
	DEACTIVATE INT DEFAULT 0,
	CreatedDate	Datetime Default Current_timestamp,
	UpdatedDate	DATETIME default current_timestamp
);
go

CREATE TABLE TB_TrialBalance(
	tb_id INT PRIMARY KEY IDENTITY(1,1),
	fileId int, 
	branch_id VARCHAR(20),
    head_id int,
	particulars_id int,
	tb_amount DECIMAL(18, 2) DEFAULT 0.0,
	tb_date datetime default current_timestamp,
	FOREIGN KEY (fileId) REFERENCES TB_FILE_MONTH_LINK(id),
	FOREIGN KEY (head_id) REFERENCES TB_MISHead(head_id),
	FOREIGN KEY (particulars_id) REFERENCES TB_Particulars(Id)
)
go

CREATE TABLE TB_error_log (
	error_process varchar(25),
	fileId int, 
    errorMessage varchar(max),
    rowNumber INTEGER,
	colNumber INTEGER,
	colName VARCHAR(25),
    error_time datetime DEFAULT current_timestamp
);
go

CREATE TABLE TB_error_log_archieve (
    error_process varchar(25),
	fileId int, 
    errorMessage varchar(max),
    rowNumber INTEGER,
	colNumber INTEGER,
	colName VARCHAR(25),
    error_time datetime DEFAULT current_timestamp,
	archieve_time datetime default current_timestamp
);
go

CREATE OR ALTER PROC dbsp_RemoveOLDTBEntries_MonthYear(@TBMONTH INT, @TBYEAR INT)
AS
BEGIN
	DELETE TB FROM TB_TrialBalance TB 
	INNER JOIN TB_FILE_MONTH_LINK TF ON TB.fileId = TF.Id
	WHERE TF.TBMonth = @TBMONTH AND TF.TBYear = @TBYEAR;

	UPDATE TB_FILE_MONTH_LINK SET DEACTIVATE = 1
	WHERE TBMonth = @TBMONTH AND TBYear = @TBYEAR AND DEACTIVATE = 0;

END
GO

CREATE OR ALTER PROC dbsp_InsertTBFileMonthYearLink
(@TBFileName	NVARCHAR(max),	@TBMonth INT, @TBYear INT, @FileId int output)
AS
	BEGIN
		  -- Declare variables to capture error details
		DECLARE @ErrorMessage NVARCHAR(4000);
		DECLARE @ErrorSeverity INT;
		DECLARE @ErrorState INT;

		BEGIN TRY
			INSERT INTO TB_FILE_MONTH_LINK (TBFileName, TBMONTH, TBYEAR)
			SELECT @TBFileName, @TBMonth, @TBYear

			SET @FileId = SCOPE_IDENTITY();

			-- Optionally check if @LastID is NULL (should not happen after a successful insert)
			IF @FileId IS NULL
			BEGIN
				RAISERROR('No identity value found after insert.', 16, 1);
				RETURN;  -- Exit the procedure
			END

			--DELETE OLD ENTRIES FOR SAME DATE IF EXISTS
			EXEC dbsp_RemoveOLDTBEntries_MonthYear @TBMONTH, @TBYEAR;

			SELECT @FileId AS FILEID;
		END TRY
		BEGIN CATCH
			-- Capture error details
			SET @ErrorMessage = ERROR_MESSAGE();
			SET @ErrorSeverity = ERROR_SEVERITY();
			SET @ErrorState = ERROR_STATE();

			-- Raise the error again with custom message
			RAISERROR('An error occurred: %s', @ErrorSeverity, @ErrorState, @ErrorMessage);
		END CATCH
END
GO

CREATE OR ALTER PROC dbsp_TBArchieveErrors
AS
	BEGIN
		INSERT INTO TB_error_log_archieve (error_process, fileId, errorMessage, rowNumber, colNumber, colName, error_time)
		SELECT error_process, fileId, errorMessage, rowNumber, colNumber, colName, error_time FROM TB_error_log;

		TRUNCATE TABLE TB_error_log;

	END
GO

CREATE OR ALTER PROC dbsp_GetTrialBalanceData(@tbMonth int, @tbYear int, @branchCode varchar(max) ='')
AS
BEGIN
	/* 
		EXEC dbsp_GetTrialBalanceData 4, 2024, 'bnlpjuls,bnahchcf,bnbgdoba'
	*/

	DECLARE @cols AS NVARCHAR(MAX), @colsNullhandling NVARCHAR(MAX);
	DECLARE @query AS NVARCHAR(MAX);

	IF (@tbMonth = 0) SET @tbMonth = MONTH(CURRENT_TIMESTAMP)
	IF @TBYEAR = 0 SET @tbYear = YEAR(CURRENT_TIMESTAMP)

	IF len(TRIM(@branchCode)) =0 
		-- Step 1: Dynamically generate the list of branch_ids for pivot columns
		SELECT	@cols = STRING_AGG(QUOTENAME(OUTLETCODE), ','),
				@colsNullhandling = STRING_AGG(CONCAT('ISNULL(',QUOTENAME(OUTLETCODE),',0) ', QUOTENAME(OUTLETCODE)), ',')
		FROM (SELECT DISTINCT OUTLETCODE FROM MST_OUTLET MO
				LEFT JOIN TB_TrialBalance TB ON MO.OUTLETCODE = TB.BRANCH_ID 
				WHERE MO.IsActive = 1
				GROUP BY OUTLETCODE
				HAVING SUM(CASE WHEN TB.tb_amount IS NOT NULL THEN 1 ELSE 0 END) > 0
			) AS Branches;
	ELSE
		BEGIN
			-- Creating @COLS by adding square brackets
			SET @COLS = '[' + REPLACE(@branchCode, ',', '],[') + ']';

			-- Creating @colsNullHandling with ISNULL function
			SET @colsNullHandling = STUFF((
				SELECT ', ISNULL(' + value + ', 0) ' + value
				FROM STRING_SPLIT(@branchCode, ',')
				FOR XML PATH(''), TYPE
				).value('.', 'NVARCHAR(MAX)'), 1, 2, '');  -- Remove the leading comma
		END

	-- Step 2: Build the dynamic SQL query for pivoting
	SET @query = '
		WITH TrialBalanceData AS (
			SELECT 
				mh.head_name,
				mh.nature,
				p.code,
				p.particulars,
				tb.branch_id,
				tb.tb_amount
			FROM TB_TrialBalance tb
			INNER JOIN TB_MISHead mh ON tb.head_id = mh.head_id
			INNER JOIN TB_Particulars p ON tb.particulars_id = p.Id
			INNER JOIN TB_FILE_MONTH_LINK tf ON TB.FILEID = TF.ID AND TF.TBMonth = '+CAST(@tbMonth AS VARCHAR(2)) +' AND TF.TBYear = '+CAST(@tbYear AS VARCHAR(4)) +'
		)
		SELECT 
			head_name,
			nature,
			code,
			particulars, ' + @colsNullhandling + '
		FROM TrialBalanceData
		PIVOT (
			SUM(tb_amount)
			FOR branch_id IN (' + @cols + ')
		) AS PivotedData
		ORDER BY head_name, code;
	';

	-- Step 3: Execute the dynamic SQL
	EXEC sp_executesql @query;
END
GO

SELECT * FROM TB_FILE_MONTH_LINK;
SELECT * FROM TB_MISHead;
SELECT * FROM TB_Particulars;
select * from TB_TrialBalance
SELECT * FROM TB_error_log;
select * from TB_error_log_archieve
go

/***
truncate table TB_error_log
truncate table TB_error_log_archieve
truncate table TB_TrialBalance;
TRUNCATE TABLE TB_FILE_MONTH_LINK
**/

