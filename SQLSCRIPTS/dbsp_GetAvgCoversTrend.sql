USE [BELLONA_LIVE]
GO
/****** Object:  StoredProcedure [dbo].[dbsp_GetAvgCoversTrend]    Script Date: 29-10-2024 00:23:27 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER    PROCEDURE [dbo].[dbsp_GetAvgCoversTrend]
(@WEEK VARCHAR(20), @FINANCIALYEAR VARCHAR(10), @branchCode varchar(20) = null, @clusterId int = 0, @cityId int = 0,
@UserId uniqueIdentifier = null, @MenuId   Int= 0 )
AS
BEGIN
	/*
		EXEC [dbo].[dbsp_GetAvgCoversTrend] @WEEK = 'WEEK 13', @FINANCIALYEAR = '2024-25'
	
	*/

	--DECLARE @WEEK VARCHAR(20) = 'WEEK 27', @FINANCIALYEAR VARCHAR(10) = '2024-25',  @branchCode varchar(20) = 'bnahisha', @clusterId int = 0, @cityId int = 0 
	DECLARE @Startdt DATETIMEOFFSET, @Enddt  DATETIMEOFFSET
	DECLARE @OUTLETS TABLE(BRANCHCODE VARCHAR(20), BRANCHNAME VARCHAR(255), CLUSTERNAME VARCHAR(255), CityName VARCHAR(100))
	DECLARE @cols AS NVARCHAR(500), @colsNullhandling NVARCHAR(1000), @query AS NVARCHAR(MAX);

	IF OBJECT_ID('tempdb..#COVER_TEMP') IS NOT NULL DROP TABLE #COVER_TEMP;
	IF @branchCode = '0' OR @branchCode = 'null' or @branchCode = '' OR LTRIM(RTRIM(@branchCode)) = ''  SET @branchCode = NULL

	SELECT @Enddt = MAX([DATE]) FROM Transaction_DateRange WHERE WeekNo = @WEEK AND FinancialYear = @FINANCIALYEAR;
	--Start date from 12 week prior
	SELECT @Startdt = DATEADD(WEEK, -12, DATEADD(WEEK, DATEDIFF(WEEK, 0, @Enddt), 0));

	INSERT @OUTLETS (BRANCHCODE, BRANCHNAME, CLUSTERNAME, CityName)
	SELECT distinct MO.OutletCode BRANCHCODE, OutletName BRANCHNAME, MC.ClusterName, MCT.CityName
	FROM MST_OUTLET MO 
	INNER JOIN MST_Clusters MC ON MO.ClusterID = MC.ClusterID
	INNER JOIN MST_Cities MCT ON MC.CityID = MCT.CityID
	INNER JOIN [dbo].[Link_Menu_Outlet] LMO on LMO.OutletId = Mo.OutletId and Menuid = (CASE WHEN @MenuId = 0 THEN MenuId ELSE @MenuId END)          
	INNER JOIN [dbo].[Um_Mst_UserAccess] UMU on UMU.AccessId = LMO.AccessId and Userid =  (CASE WHEN @UserId IS NULL THEN UserId ELSE @UserId END)
	WHERE	--MO.IsActive = 1 AND	
			MC.CityID =  (CASE WHEN ISNULL(@cityId, 0) = 0 THEN MC.CityID ELSE @cityId END)
			AND MO.ClusterID = (CASE WHEN ISNULL(@clusterId, 0) = 0 THEN MO.ClusterID ELSE @clusterId END)
			AND OutletCode = (CASE WHEN ISNULL(@branchCode , '0') = '0' THEN OutletCode ELSE @branchCode END)  

	;WITH CTE_TEMP AS(
		SELECT	TDR.WeekNo,
				SUM(SI.PERSONCOUNT) TotalCovers, 
				SUM(SI.NETAMOUNT) TotalSale
		FROM Rista_SaleInvoices (NOLOCK) SI
		INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
		INNER JOIN Transaction_DateRange (NOLOCK) TDR ON SI.[InvoiceDay] = TDR.[Date] 
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) SD  ON SI.InvoiceID = SD.InvoiceID
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
			AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled'
		GROUP BY TDR.WeekNo
	)
	
	SELECT DISTINCT TDR.WeekNo InvoiceDay, ISNULL(TotalSale /NULLIF(TotalCovers, 0), 0) ApcDineIn, ISNULL(TotalCovers,0) TotalCovers, ISNULL(TotalSale, 0) TotalSale
	FROM Transaction_DateRange TDR 
	LEFT JOIN CTE_TEMP S ON TDR.WeekNo = S.WeekNo
	WHERE TDR.[Date] BETWEEN @Startdt AND @Enddt
		AND ((SELECT COUNT(1) FROM CTE_TEMP) > 0)

	--	SELECT WeekNo InvoiceDay, ISNULL(TotalSale /NULLIF(TotalCovers, 0), 0) ApcDineIn, TotalCovers, TotalSale
	--FROM CTE_TEMP;

END
