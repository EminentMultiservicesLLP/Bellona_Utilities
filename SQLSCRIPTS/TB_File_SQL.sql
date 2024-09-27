/***
drop table TB_TrialBalance;
drop table TB_Particulars;
drop table TB_MISHead;
drop table TB_error_log;
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
	CreatedDate	Datetime Default Current_timestamp
);
go
CREATE TABLE TB_TrialBalance(
	tb_id INT PRIMARY KEY IDENTITY(1,1),
	fileId int, 
	branch_id VARCHAR(20),
    head_id int,
	particulars_id int,
	tb_date DATETIMEOFFSET,
    tb_amount DECIMAL(18, 2) DEFAULT 0.0,
	FOREIGN KEY (head_id) REFERENCES TB_MISHead(head_id),
	FOREIGN KEY (particulars_id) REFERENCES TB_Particulars(Id)
)
go

CREATE TABLE TB_error_log (
    id INTEGER PRIMARY KEY  IDENTITY(1,1),
	error_process varchar(25),
	fileId int, 
    errorMessage varchar(max),
    rowNumber INTEGER,
	colNumber INTEGER,
	colName VARCHAR(25),
    error_time datetime DEFAULT current_timestamp
);
go




SELECT * FROM TB_FILE_MONTH_LINK;
SELECT * FROM TB_MISHead;
SELECT * FROM TB_Particulars;
SELECT * FROM TB_TrialBalance;
SELECT * FROM TB_error_log




/***
truncate table TB_error_log
truncate table TB_TrialBalance;
truncate table TB_Particulars;
truncate table TB_MISHead;
TRUNCATE TABLE TB_FILE_MONTH_LINK
**/

CREATE PROC dbsp_InsertTBFileMonthYearLink(@TBFileName	NVARCHAR(max),	@TBMonth INT, @TBYear INT)
AS
	BEGIN
		  -- Declare variables to capture error details
		DECLARE @ErrorMessage NVARCHAR(4000);
		DECLARE @ErrorSeverity INT;
		DECLARE @ErrorState INT;

		BEGIN TRY
			DECLARE @LastID INT

			INSERT INTO TB_FILE_MONTH_LINK (TBFileName, TBMONTH, TBYEAR)
			SELECT @TBFileName, @TBMonth, @TBYear

			SET @LastID = SCOPE_IDENTITY();

			-- Optionally check if @LastID is NULL (should not happen after a successful insert)
			IF @LastID IS NULL
			BEGIN
				RAISERROR('No identity value found after insert.', 16, 1);
				RETURN;  -- Exit the procedure
			END

			SELECT @LastID AS FILEID;
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

