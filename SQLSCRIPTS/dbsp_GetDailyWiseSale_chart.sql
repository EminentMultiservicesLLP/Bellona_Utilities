USE [BELLONA_LIVE]
GO
/****** Object:  StoredProcedure [dbo].[dbsp_GetDailyWiseSale_chart]    Script Date: 15-10-2024 14:42:34 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER  PROCEDURE [dbo].[dbsp_GetDailyWiseSale_chart]
(@WEEK VARCHAR(20), @FINANCIALYEAR VARCHAR(10), @branchCode varchar(20) = null, @clusterId int = 0, @cityId int = 0,
@UserId uniqueIdentifier = null, @MenuId   Int= 0)
AS
BEGIN
	/*
		2024-01-15 00:00:00.0000000 +00:00	2024-04-07 00:00:00.0000000 +00:00
		EXEC [dbo].[dbsp_GetDailyWiseSale_chart] @WEEK = 'WEEK 30', @FINANCIALYEAR = '2024-25',  @branchCode = 'bnkrisha'
	*/

	--DECLARE @WEEK VARCHAR(20) = 'WEEK 27', @FINANCIALYEAR VARCHAR(10) = '2024-25',  @branchCode varchar(20) = 'bnkrisha', @clusterId int = 0, @cityId int = 0 
	DECLARE @Startdt DATETIMEOFFSET, @Enddt  DATETIMEOFFSET, @ActualNetAmount Decimal(15,2)
	DECLARE @OUTLETS TABLE(OutletId INT, BRANCHCODE VARCHAR(20), BRANCHNAME VARCHAR(255), CLUSTERNAME VARCHAR(255), CityName VARCHAR(100))

	IF @branchCode = '0' OR @branchCode = 'null' or @branchCode = '' OR LTRIM(RTRIM(@branchCode)) = ''  SET @branchCode = NULL

	SELECT @Enddt = MAX([DATE]), @Startdt = MIN([DATE]) FROM Transaction_DateRange WHERE WeekNo = @WEEK AND FinancialYear = @FINANCIALYEAR;
	
	INSERT @OUTLETS (OutletId, BRANCHCODE, BRANCHNAME, CLUSTERNAME, CityName)
	SELECT distinct MO.OutletId, OutletCode BRANCHCODE, OutletName BRANCHNAME, MC.ClusterName, MCT.CityName
	FROM MST_OUTLET MO 
	INNER JOIN MST_Clusters MC ON MO.ClusterID = MC.ClusterID
	INNER JOIN MST_Cities MCT ON MC.CityID = MCT.CityID
	INNER JOIN [dbo].[Link_Menu_Outlet] LMO on LMO.OutletId = Mo.OutletId and Menuid = (CASE WHEN @MenuId = 0 THEN MenuId ELSE @MenuId END)          
	INNER JOIN [dbo].[Um_Mst_UserAccess] UMU on UMU.AccessId = LMO.AccessId and Userid =  (CASE WHEN @UserId IS NULL THEN UserId ELSE @UserId END)
	WHERE	MC.CityID =  (CASE WHEN ISNULL(@cityId, 0) = 0 THEN MC.CityID ELSE @cityId END)
			AND MO.ClusterID = (CASE WHEN ISNULL(@clusterId, 0) = 0 THEN MO.ClusterID ELSE @clusterId END)
			AND OutletCode = (CASE WHEN ISNULL(@branchCode , '0') = '0' THEN OutletCode ELSE @branchCode END) ;

	SELECT @ActualNetAmount = SUM(SI.NetAmount) + SUM(SI.NetChargeAmount)
	FROM  Rista_SaleInvoices (NOLOCK) SI
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE 
	INNER JOIN Transaction_DateRange (NOLOCK) TDR ON SI.InvoiceDay = TDR.[Date] 
	WHERE	SI.InvoiceDay BETWEEN @Startdt AND @Enddt
			AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled';
	
	;WITH CTE_NETSALE AS (
		SELECT SI.InvoiceDay [DATE], SUM(SI.NetAmount + SI.NetChargeAmount) NETAMOUNT
		FROM  Rista_SaleInvoices (NOLOCK) SI
		INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE 
		WHERE	SI.InvoiceDay BETWEEN @Startdt AND @Enddt
				AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
		GROUP BY SI.InvoiceDay
	)
	SELECT DISTINCT s.[date] [DATE], ISNULL(S.NETAMOUNT, 0) NETAMOUNT, ((ISNULL(S.NETAMOUNT, 0) / NULLIF(@ActualNetAmount, 0)) * 100) NETPERC
	FROM CTE_NETSALE S;
	
END
GO





