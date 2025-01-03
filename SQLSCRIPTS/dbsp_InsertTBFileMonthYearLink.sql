USE [BELLONA_LIVE]
GO
/****** Object:  StoredProcedure [dbo].[dbsp_InsertTBFileMonthYearLink]    Script Date: 17-10-2024 07:30:57 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER   PROC [dbo].[dbsp_InsertTBFileMonthYearLink]
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

			UPDATE TB_FILE_MONTH_LINK SET DEACTIVATE = 1
			WHERE TBMonth = @TBMonth AND TBYear = @TBYear AND Id <> @FileId;

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
