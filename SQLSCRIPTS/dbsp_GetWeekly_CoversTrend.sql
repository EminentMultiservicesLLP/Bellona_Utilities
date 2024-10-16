USE [BELLONA_LIVE]
GO
/****** Object:  StoredProcedure [dbo].[dbsp_GetDSR_Comparision_Daywise]    Script Date: 15-10-2024 14:42:34 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER  PROCEDURE [dbo].[dbsp_GetWeekly_CoversTrend]
(@WEEK VARCHAR(20), @FINANCIALYEAR VARCHAR(10), @branchCode varchar(20) = null, @clusterId int = 0, @cityId int = 0 )
AS
BEGIN
	/*
		EXEC [dbo].[dbsp_GetWeekly_CoversTrend] @WEEK = 'WEEK 27', @FINANCIALYEAR = '2024-25',  @branchCode = 'bnahisha'
		EXEC [dbo].[dbsp_GetDSR_Comparision_Daywise] @USERDATE = '2024-09-14'
	*/

	--DECLARE @WEEK VARCHAR(20) = 'WEEK 27', @FINANCIALYEAR VARCHAR(10) = '2024-25',  @branchCode varchar(20) = 'bnahisha', @clusterId int = 0, @cityId int = 0 
	DECLARE @Startdt DATETIMEOFFSET, @Enddt  DATETIMEOFFSET
	DECLARE @OUTLETS TABLE(BRANCHCODE VARCHAR(20), BRANCHNAME VARCHAR(255), CLUSTERNAME VARCHAR(255), CityName VARCHAR(100))
	DECLARE @cols AS NVARCHAR(500), @colsNullhandling NVARCHAR(1000), @query AS NVARCHAR(MAX);

	IF OBJECT_ID('tempdb..#COVER_TEMP') IS NOT NULL DROP TABLE #COVER_TEMP;
	IF @branchCode = '0' SET @branchCode = NULL

	--SELECT * FROM Transaction_DateRange WHERE WeekNo = 'WEEK 27'
	SELECT @Startdt= MIN([DATE]), @Enddt = MAX([DATE]) FROM Transaction_DateRange WHERE WeekNo = @WEEK AND FinancialYear = @FINANCIALYEAR

	INSERT @OUTLETS (BRANCHCODE, BRANCHNAME, CLUSTERNAME, CityName)
	SELECT distinct OutletCode BRANCHCODE, OutletName BRANCHNAME, MC.ClusterName, MCT.CityName
	FROM MST_OUTLET MO 
	INNER JOIN MST_Clusters MC ON MO.ClusterID = MC.ClusterID
	INNER JOIN MST_Cities MCT ON MC.CityID = MCT.CityID
	WHERE	MO.IsActive = 1 	
			AND MC.CityID =  (CASE WHEN ISNULL(@cityId, 0) = 0 THEN MC.CityID ELSE @cityId END)
			AND MO.ClusterID = (CASE WHEN ISNULL(@clusterId, 0) = 0 THEN MO.ClusterID ELSE @clusterId END)
			AND OutletCode = (CASE WHEN ISNULL(@branchCode , '0') = '0' THEN OutletCode ELSE @branchCode END)  

	SELECT
		SI.InvoiceDay,
		MST.SessionName,
		MAX(SI.PersonCount) AS COVERS
	INTO #COVER_TEMP
	FROM  MST_SaleTimeSessions (NOLOCK) MST
	INNER JOIN Rista_SaleInvoices (NOLOCK) SI ON CAST(SI.InvoiceDate AS TIME) BETWEEN MST.StartTime AND MST.EndTime
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
	WHERE SI.InvoiceDay BETWEEN @Startdt AND @Enddt
	GROUP BY MST.SessionName, SI.InvoiceDay

	--SELECT @cols = STRING_AGG(''''+ SessionName, ''',') WITHIN GROUP (ORDER BY starttime) FROM MST_SaleTimeSessions;
	SELECT @colsNullhandling = STRING_AGG('ISNULL(['+[InvoiceDay] +'], 0) AS ['+[InvoiceDay] +']', ',') WITHIN GROUP (ORDER BY InvoiceDay),
		   @cols = 	STRING_AGG('['+[InvoiceDay] +']', ',') WITHIN GROUP (ORDER BY InvoiceDay)
	FROM (
		SELECT DISTINCT CONVERT(VARCHAR, [InvoiceDay], 120) AS [InvoiceDay] FROM #COVER_TEMP WHERE [InvoiceDay] IS NOT NULL 
	) AS DistinctInvoiceDays;

	SET @query = '
		;WITH CTE_PVT AS(
			SELECT 
				A.SessionName, T.InvoiceDay, COALESCE(T.COVERS, 0) AS COVERS, A.StartTime
			FROM MST_SaleTimeSessions A
			LEFT JOIN #COVER_TEMP T ON A.SessionName = T.SessionName 
		)
		SELECT SessionName, '+ @colsNullhandling +' FROM CTE_PVT
		PIVOT (
			MAX(Covers)
			FOR InvoiceDay IN ('+ @cols +')
		) AS PivotedData
		ORDER BY StartTime
		'
	
	--PRINT(@query)
	EXEC sp_executesql @query;
END
GO





