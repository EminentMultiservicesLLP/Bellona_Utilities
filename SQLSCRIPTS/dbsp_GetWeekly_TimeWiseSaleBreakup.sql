USE [Bellona_Dev]
GO
/****** Object:  StoredProcedure [dbo].[dbsp_GetWeekly_TimeWiseSaleBreakup]    Script Date: 31-10-2024 18:28:02 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER   PROCEDURE [dbo].[dbsp_GetWeekly_TimeWiseSaleBreakup]
(@WEEK VARCHAR(20), @FINANCIALYEAR VARCHAR(10), @branchCode varchar(20) = null, @clusterId int = 0, @cityId int = 0,
@UserId uniqueIdentifier = null, @MenuId   Int= 0 )
AS
BEGIN
	/*
		EXEC [dbo].[dbsp_GetWeekly_TimeWiseSaleBreakup] @WEEK = 'WEEK 29', @FINANCIALYEAR = '2024-25',  @branchCode = 'bnmadoba'
		
		DECLARE @WEEK VARCHAR(20) = 'WEEK 27', @FINANCIALYEAR VARCHAR(10) = '2024-25',  @branchCode varchar(20) = 'bnahisha', @clusterId int = 0, @cityId int = 0 
	*/
	DECLARE @Startdt DATETIMEOFFSET, @Enddt  DATETIMEOFFSET
	DECLARE @OUTLETS TABLE(BRANCHCODE VARCHAR(20), BRANCHNAME VARCHAR(255), CLUSTERNAME VARCHAR(255), CityName VARCHAR(100))
	DECLARE @cols AS NVARCHAR(500), @colsNullhandling NVARCHAR(1000), @query AS NVARCHAR(MAX);

	IF OBJECT_ID('tempdb..#TIMEWISE_TEMP') IS NOT NULL DROP TABLE #TIMEWISE_TEMP;
	CREATE TABLE #TIMEWISE_TEMP (SESSIONNAME VARCHAR(50)  COLLATE SQL_Latin1_General_CP1_CI_AS, 
								NETAMOUNT DECIMAL(15,2))

	IF @branchCode = '0' OR @branchCode = 'null' or @branchCode = '' OR LTRIM(RTRIM(@branchCode)) = ''  SET @branchCode = NULL

	--SELECT * FROM Transaction_DateRange WHERE WeekNo = 'WEEK 27'
	SELECT @Startdt= MIN([DATE]), @Enddt = MAX([DATE]) FROM Transaction_DateRange WHERE WeekNo = @WEEK AND FinancialYear = @FINANCIALYEAR

	INSERT @OUTLETS (BRANCHCODE, BRANCHNAME, CLUSTERNAME, CityName)
	SELECT distinct OutletCode BRANCHCODE, OutletName BRANCHNAME, MC.ClusterName, MCT.CityName
	FROM MST_OUTLET MO 
	INNER JOIN MST_Clusters MC ON MO.ClusterID = MC.ClusterID
	INNER JOIN MST_Cities MCT ON MC.CityID = MCT.CityID
	INNER JOIN [dbo].[Link_Menu_Outlet] LMO on LMO.OutletId = Mo.OutletId and Menuid = (CASE WHEN @MenuId = 0 THEN MenuId ELSE @MenuId END)          
	INNER JOIN [dbo].[Um_Mst_UserAccess] UMU on UMU.AccessId = LMO.AccessId and Userid =  (CASE WHEN @UserId IS NULL THEN UserId ELSE @UserId END)
	WHERE	MC.CityID =  (CASE WHEN ISNULL(@cityId, 0) = 0 THEN MC.CityID ELSE @cityId END)
			AND MO.ClusterID = (CASE WHEN ISNULL(@clusterId, 0) = 0 THEN MO.ClusterID ELSE @clusterId END)
			AND OutletCode = (CASE WHEN ISNULL(@branchCode , '0') = '0' THEN OutletCode ELSE @branchCode END)  

	INSERT INTO #TIMEWISE_TEMP
	SELECT 	MST.SessionName, SUM(SI.NetAmount)
		FROM  MST_SaleTimeSessions (NOLOCK) MST
		INNER JOIN Rista_SaleInvoices (NOLOCK) SI 
				ON (CONVERT(TIME, SI.InvoiceDate) BETWEEN MST.StartTime AND MST.EndTime)
				OR (MST.StartTime > MST.EndTime 
					AND (	CONVERT(TIME, SI.InvoiceDate) >= MST.StartTime 
							OR CONVERT(TIME, SI.InvoiceDate) < MST.EndTime))
		INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
		WHERE SI.InvoiceDay BETWEEN @Startdt AND @Enddt
		GROUP BY MST.SessionName

	;WITH CTE_TOTALSALE AS ( SELECT SUM(NETAMOUNT) TOTAL FROM #TIMEWISE_TEMP)
	SELECT	CONCAT(MST.SessionName, ' (', CONVERT(varchar, StartTime, 100), ' - ', CONVERT(varchar, EndTime, 100), ')'), 
			ISNULL(NETAMOUNT, 0) SESSION_NETAMOUNT, ISNULL(TOTAL, 0) TOTAL_NETAMOUNT,
			CASE	WHEN ISNULL(TOTAL, 0) > 0 THEN (ISNULL(NETAMOUNT, 0) / NULLIF(B.TOTAL, 0) * 100)
					ELSE 0 END AS Percentage
	FROM	MST_SaleTimeSessions (NOLOCK) MST 
	LEFT JOIN #TIMEWISE_TEMP A ON MST.SessionName = A.SessionName
	CROSS JOIN CTE_TOTALSALE B
	WHERE (SELECT COUNT(1) FROM #TIMEWISE_TEMP) > 0;
	
END
