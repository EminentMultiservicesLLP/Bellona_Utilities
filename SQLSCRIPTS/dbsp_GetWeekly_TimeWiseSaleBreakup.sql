
USE [BELLONA_LIVE]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER  PROCEDURE [dbo].[dbsp_GetWeekly_TimeWiseSaleBreakup]
(@WEEK VARCHAR(20), @FINANCIALYEAR VARCHAR(10), @branchCode varchar(20) = null, @clusterId int = 0, @cityId int = 0 )
AS
BEGIN
	/*
		EXEC [dbo].[dbsp_GetWeekly_TimeWiseSaleBreakup] @WEEK = 'WEEK 27', @FINANCIALYEAR = '2024-25',  @branchCode = 'bnahisha'
		
		DECLARE @WEEK VARCHAR(20) = 'WEEK 27', @FINANCIALYEAR VARCHAR(10) = '2024-25',  @branchCode varchar(20) = 'bnahisha', @clusterId int = 0, @cityId int = 0 
	*/
	DECLARE @Startdt DATETIMEOFFSET, @Enddt  DATETIMEOFFSET
	DECLARE @OUTLETS TABLE(BRANCHCODE VARCHAR(20), BRANCHNAME VARCHAR(255), CLUSTERNAME VARCHAR(255), CityName VARCHAR(100))
	DECLARE @cols AS NVARCHAR(500), @colsNullhandling NVARCHAR(1000), @query AS NVARCHAR(MAX);

	IF OBJECT_ID('tempdb..#SALEBREAKUP_TEMP') IS NOT NULL DROP TABLE #SALEBREAKUP_TEMP;
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

	;WITH CTE_TIMESALE AS(
		SELECT
			MST.SessionName,
			SUM(SI.NetAmount) AS NETAMOUNT
		FROM  MST_SaleTimeSessions (NOLOCK) MST
		INNER JOIN Rista_SaleInvoices (NOLOCK) SI ON CAST(SI.InvoiceDate AS TIME) BETWEEN MST.StartTime AND MST.EndTime
		INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
		WHERE SI.InvoiceDay BETWEEN @Startdt AND @Enddt
		GROUP BY MST.SessionName
	),
	CTE_TOTALSALE AS ( SELECT SUM(NETAMOUNT) TOTAL FROM CTE_TIMESALE)
	SELECT	MST.SessionName, 
			ISNULL(NETAMOUNT, 0) SESSION_NETAMOUNT, ISNULL(TOTAL, 0) TOTAL_NETAMOUNT,
			CASE	WHEN ISNULL(TOTAL, 0) > 0 THEN (ISNULL(NETAMOUNT, 0) / B.TOTAL * 100)
					ELSE 0 END AS Percentage
	FROM	MST_SaleTimeSessions (NOLOCK) MST 
	LEFT JOIN CTE_TIMESALE A ON MST.SessionName = A.SessionName
	CROSS JOIN CTE_TOTALSALE B;
END
GO



