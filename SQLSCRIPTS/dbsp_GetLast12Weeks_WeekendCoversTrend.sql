USE [Bellona_Dev]
GO
/****** Object:  StoredProcedure [dbo].[dbsp_GetLast12Weeks_WeekendCoversTrend]    Script Date: 05-11-2024 00:03:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [dbo].[dbsp_GetLast12Weeks_WeekendCoversTrend]
(@WEEK VARCHAR(20), @FINANCIALYEAR VARCHAR(10), @branchCode varchar(20) = null, @clusterId int = 0, @cityId int = 0, @brandId int = 0,
@UserId uniqueIdentifier = null, @MenuId Int= 0 )
AS
BEGIN
	/*
		EXEC [dbo]. [dbsp_GetLast12Weeks_WeekendCoversTrend] @WEEK = 'WEEK 29', @FINANCIALYEAR = '2024-25', @branchCode = 'bnmadoba'
	*/

--DECLARE @WEEK VARCHAR(20) = 'WEEK 27', @FINANCIALYEAR VARCHAR(10) = '2024-25', @branchCode varchar(20) = 'bnahisha', @clusterId int = 0, @cityId int = 0 
	DECLARE @Startdt DATETIMEOFFSET, @Enddt DATETIMEOFFSET
	DECLARE @OUTLETS TABLE(BRANCHCODE VARCHAR(20), BRANCHNAME VARCHAR(255), CLUSTERNAME VARCHAR(255), CityName VARCHAR(100))
	DECLARE @cols AS NVARCHAR(max), @colsNullhandling NVARCHAR(max), @query AS NVARCHAR(MAX);

IF OBJECT_ID('tempdb.. #COVER_TEMP') IS NOT NULL DROP TABLE #COVER_TEMP;
	CREATE TABLE #COVER_TEMP (InvoiceDay VARCHAR(25) COLLATE SQL_Latin1_General_CP1_CI_AS, SESSIONNAME VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS, COVERS INT)

IF @branchCode = '0' OR @branchCode = 'null' or @branchCode = '' OR LTRIM(RTRIM(@branchCode)) = '' SET @branchCode = NULL
		
SELECT @Enddt = MAX([DATE]) FROM Transaction_DateRange WHERE WeekNo = @WEEK AND FinancialYear = @FINANCIALYEAR;
	--Start date from 12 week prior
	SELECT @Startdt = DATEADD(WEEK, -12, DATEADD(WEEK, DATEDIFF(WEEK, 0, @Enddt), 0));

INSERT @OUTLETS (BRANCHCODE, BRANCHNAME, CLUSTERNAME, CityName)
	SELECT distinct OutletCode BRANCHCODE, OutletName BRANCHNAME, MC.ClusterName, MCT. CityName
	FROM MST_OUTLET MO 
	INNER JOIN MST_Clusters MC ON MO. ClusterID = MC.ClusterID
	INNER JOIN MST_Cities MCT ON MC.CityID = MCT. CityID
	INNER JOIN [dbo]. [Link_Menu_Outlet] LMO on LMO. OutletId = Mo.OutletId and Menuid = (CASE WHEN @MenuId = 0 THEN MenuId ELSE @MenuId END)          
	INNER JOIN [dbo]. [Um_Mst_UserAccess] UMU on UMU. AccessId = LMO. AccessId and Userid = (CASE WHEN @UserId IS NULL THEN UserId ELSE @UserId END)
	WHERE MC.CityID = (CASE WHEN ISNULL(@cityId, 0) = 0 THEN MC.CityID ELSE @cityId END)
			AND MO. BrandID = (CASE WHEN ISNULL(@brandId, 0) = 0 THEN MO. BrandID ELSE @brandId END)
			AND MO. ClusterID = (CASE WHEN ISNULL(@clusterId, 0) = 0 THEN MO. ClusterID ELSE @clusterId END)
			AND OutletCode = (CASE WHEN ISNULL(@branchCode , '0') = '0' THEN OutletCode ELSE @branchCode END)  
	
INSERT INTO #COVER_TEMP
	SELECT
		TDR.[WeekNo] as InvoiceDay,
		MST. SessionName,
		SUM(SI. PersonCount) AS COVERS
	FROM MST_SaleTimeSessions (NOLOCK) MST
	INNER JOIN Rista_SaleInvoices (NOLOCK) SI 
			ON (CONVERT(TIME, SI. InvoiceDate) BETWEEN MST. StartTime AND MST. EndTime)
				OR (MST. StartTime > MST. EndTime 
					AND (CONVERT(TIME, SI. InvoiceDate) >= MST. StartTime 
							OR CONVERT(TIME, SI. InvoiceDate) < MST. EndTime))
	INNER JOIN Transaction_DateRange (NOLOCK) TDR ON SI. [InvoiceDay] = TDR. [Date] 
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO. BRANCHCODE AND SI. InvoiceType <> 'NC' AND SI. Status <> 'Cancelled'
	WHERE SI. InvoiceDay BETWEEN @Startdt AND @Enddt AND TDR.Days IN ('SATURDAY','SUNDAY')
	GROUP BY MST. SessionName, tdr.[WeekNo]

UPDATE CT
	SET CT. SESSIONNAME = CONCAT(CT. SESSIONNAME, ' (', CONVERT(varchar, StartTime, 100), ' - ', CONVERT(varchar, EndTime, 100), ')')
	FROM #COVER_TEMP CT
	INNER JOIN MST_SaleTimeSessions (NOLOCK) MST ON MST. SessionName = CT. SESSIONNAME

INSERT INTO #COVER_TEMP
	SELECT InvoiceDay, 'TOTAL', SUM(COVERS) FROM #COVER_TEMP GROUP BY InvoiceDay order by InvoiceDay

--SELECT @cols = STRING_AGG(''''+ SessionName, ''',') WITHIN GROUP (ORDER BY starttime) FROM MST_SaleTimeSessions;
	SELECT @colsNullhandling = STRING_AGG('ISNULL(['+[InvoiceDay] +'], 0) AS ['+[InvoiceDay] +']', ',') WITHIN GROUP (ORDER BY InvoiceDay),
		   @cols = STRING_AGG('['+[InvoiceDay] +']', ',') WITHIN GROUP (ORDER BY InvoiceDay)
	FROM (
		SELECT DISTINCT CONVERT(VARCHAR, InvoiceDay, 120) AS [InvoiceDay] FROM #COVER_TEMP WHERE [InvoiceDay] IS NOT NULL 
	) AS DistinctInvoiceDays;
	   	
--SELECt * from #COVER_TEMP
	SET @query = '
		; WITH CTE_PVT AS(
			SELECT 
				T.SessionName, T.InvoiceDay, COALESCE(T.COVERS, 0) AS COVERS, '''' STARTTIME
			FROM #COVER_TEMP T
		)
		SELECT SessionName, '+ @colsNullhandling +' FROM CTE_PVT
		PIVOT (
			MAX(Covers)
			FOR InvoiceDay IN ('+ @cols +')
		) AS PivotedData
		ORDER BY StartTime
		'	
	PRINT(@query)
	EXEC sp_executesql @query;

--EXEC [dbo]. [dbsp_GetLast12Weeks_CoversTrend] @WEEK = 'WEEK 29', @FINANCIALYEAR = '2024-25', @branchCode = 'bnmadoba'
END
