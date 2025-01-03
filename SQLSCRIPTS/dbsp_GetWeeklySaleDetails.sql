USE [BELLONA_LIVE]
GO
/****** Object:  StoredProcedure [dbo].[dbsp_GetWeeklySaleDetails]    Script Date: 29-10-2024 00:00:15 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


--SELECT * FROM Transaction_DateRange


ALTER    PROCEDURE [dbo].[dbsp_GetWeeklySaleDetails]
( @WEEK VARCHAR(20), @branchCode varchar(20) = null, @clusterId int = 0, @cityId int = 0,
	@UserId   uniqueIdentifier = null, @MenuId   Int= 0        
 )
AS
BEGIN
	/*
		EXEC [dbo].[dbsp_GetWeeklySaleDetails] @WEEK = 'WEEK 27'
	*/
	DECLARE @OUTLETS TABLE(BRANCHCODE VARCHAR(20), BRANCHNAME VARCHAR(255), CLUSTERNAME VARCHAR(255), CityName VARCHAR(100))
	DECLARE @Startdt datetime, @Enddt  datetime

	IF @branchCode = '0' OR @branchCode = 'null' or @branchCode = '' OR LTRIM(RTRIM(@branchCode)) = ''  SET @branchCode = NULL

	SELECT @Startdt = MIN([DATE]), @Enddt = MAX([DATE]) FROM Transaction_DateRange WHERE [WeekNo] = @WEEK and [FinancialYear] = '2024-25';

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
	
	;with cte_temp as (
		SELECT 	max(SI.InvoiceDay) invoiceday,
			SUM(CASE WHEN AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) AS FoodSale,
			SUM(CASE WHEN AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) AS BeverageSale,
			SUM(CASE WHEN AccountName = 'LIQUOR SALE' THEN SIT.NetAmount ELSE 0 END) AS LiquorSale,
			SUM(CASE WHEN AccountName = 'TOBACCO SALE' THEN SIT.NetAmount ELSE 0 END) AS TobaccoSale,
			SUM(CASE WHEN AccountName NOT IN ('Food Sale', 'Beverage Sale', 'TOBACCO SALE', 'LIQUOR SALE') THEN SIT.NetAmount ELSE 0 END) AS OtherSale
		--INTO #CTE_ITEM_SALE_TEMP 
		FROM Rista_SaleInvoices (NOLOCK) SI 
		INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
		INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
		WHERE	SI.InvoiceDay BETWEEN @Startdt and @Enddt
				AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
		GROUP BY SI.InvoiceDay
	),
	CTE_TEMP1 AS(
	SELECT	max(invoiceday) invoiceday, sum(foodsale) foodsale, sum(BeverageSale) BeverageSale, sum(LiquorSale) LiquorSale, sum(TobaccoSale) TobaccoSale, sum(OtherSale) OtherSale
	FROM cte_temp)
	SELECT  invoiceday, foodsale, BeverageSale, LiquorSale, TobaccoSale, OtherSale FROM CTE_TEMP1
	UNION ALL
	SELECT	INVOICEDAY, ISNULL(FoodSale / NULLIF(FoodSale + LiquorSale + BeverageSale + TobaccoSale + OtherSale, 0), 0) * 100,
			ISNULL(LiquorSale / NULLIF(FoodSale + LiquorSale + BeverageSale + TobaccoSale + OtherSale, 0), 0)* 100,
			ISNULL(BeverageSale / NULLIF(FoodSale + LiquorSale + BeverageSale + TobaccoSale + OtherSale, 0), 0)* 100,
			ISNULL(TobaccoSale / NULLIF(FoodSale + LiquorSale + BeverageSale + TobaccoSale + OtherSale, 0), 0)* 100,
			ISNULL(OtherSale / NULLIF(FoodSale + LiquorSale + BeverageSale + TobaccoSale + OtherSale, 0), 0)* 100
	FROM CTE_TEMP1
	
	
	
END
