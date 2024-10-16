USE [BELLONA_LIVE]
GO
/****** Object:  StoredProcedure [dbo].[dbsp_RemoveOLDTBEntries_MonthYear]    Script Date: 17-10-2024 07:35:10 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROC [dbo].[dbsp_RemoveOLDTBEntries_MonthYear](@TBMONTH INT, @TBYEAR INT)
AS
BEGIN
	DELETE TB FROM TB_TrialBalance TB 
	INNER JOIN TB_FILE_MONTH_LINK TF ON TB.fileId = TF.Id
	WHERE TF.TBMonth = @TBMONTH AND TF.TBYear = @TBYEAR;

END
GO
