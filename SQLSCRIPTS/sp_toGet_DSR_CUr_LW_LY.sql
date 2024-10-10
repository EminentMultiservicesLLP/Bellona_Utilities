
-- DATE, DAY, WEEK WILL BE PASSED
-- CURRENT DAY DATA NEE TO SHOW
DECLARE @USERDATE DATETIME = '2024-09-14',   @branchCode varchar(20) = 'bnahisha', @clusterId int = 0, @cityId int = 0 
DECLARE @Startdt datetime, @Enddt  datetime
DECLARE @CUR_DAY VARCHAR(20), @CUR_WEEK VARCHAR(20)
DECLARE @LW_DAY VARCHAR(20), @LW_WEEK VARCHAR(20), @LW_DATE DATETIME
DECLARE @LY_DAY VARCHAR(20), @LY_WEEK VARCHAR(20), @LY_DATE DATETIME
DECLARE @OUTLETS TABLE(BRANCHCODE VARCHAR(20), BRANCHNAME VARCHAR(255), CLUSTERNAME VARCHAR(255), CityName VARCHAR(100))

IF OBJECT_ID('tempdb..#CUR_SALE_TEMP') IS NOT NULL DROP TABLE #CUR_SALE_TEMP;
IF OBJECT_ID('tempdb..#LW_SALE_TEMP') IS NOT NULL DROP TABLE #LW_SALE_TEMP;
IF OBJECT_ID('tempdb..#LY_SALE_TEMP') IS NOT NULL DROP TABLE #LY_SALE_TEMP;

--SELECT  * FROM Transaction_DateRange
SELECT @CUR_DAY = [Days], @CUR_WEEK = WeekNo  FROM Transaction_DateRange WHERE [DATE] = @USERDATE;
SELECT @LW_DAY = [Days], @LW_WEEK = WeekNo, @LW_DATE = [DATE]  FROM Transaction_DateRange WHERE [DATE] = DATEADD(DAY, -7, @USERDATE);
SELECT @LY_DAY = [Days], @LY_WEEK = WeekNo, @LY_DATE = [DATE]  FROM Transaction_DateRange 
	WHERE [DAYS] = @CUR_DAY AND [MONTH] = MONTH(@USERDATE)-2;
	--WHERE WeekNo = @CUR_WEEK AND [DAYS] = @CUR_DAY AND [YEAR] = YEAR(@USERDATE)-1;

IF @branchCode = '0' SET @branchCode = NULL

INSERT @OUTLETS (BRANCHCODE, BRANCHNAME, CLUSTERNAME, CityName)
SELECT distinct OutletCode BRANCHCODE, OutletName BRANCHNAME, MC.ClusterName, MCT.CityName
FROM MST_OUTLET MO 
INNER JOIN MST_Clusters MC ON MO.ClusterID = MC.ClusterID
INNER JOIN MST_Cities MCT ON MC.CityID = MCT.CityID
WHERE	MC.CityID =  (CASE WHEN ISNULL(@cityId, 0) = 0 THEN MC.CityID ELSE @cityId END)
		AND MO.ClusterID = (CASE WHEN ISNULL(@clusterId, 0) = 0 THEN MO.ClusterID ELSE @clusterId END)
		AND OutletCode = (CASE WHEN ISNULL(@branchCode , '0') = '0' THEN OutletCode ELSE @branchCode END)  


SELECT @CUR_DAY, @CUR_WEEK, @LW_DAY, @LW_WEEK, @LW_DATE, @LY_DAY, @LY_WEEK, @LY_DATE
--SELECT * FROM @OUTLETS

--SELECT 	SI.InvoiceDay,
--	SUM(SI.PersonCount) COVERS,
--	SUM(CASE WHEN AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) AS FoodSale,
--	SUM(CASE WHEN AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) AS BeverageSale,
--	SUM(CASE WHEN AccountName = 'LIQUOR SALE' THEN SIT.NetAmount ELSE 0 END) AS LiquorSale,
--	SUM(CASE WHEN AccountName = 'TOBACCO SALE' THEN SIT.NetAmount ELSE 0 END) AS TobaccoSale,
--	SUM(CASE WHEN AccountName NOT IN ('Food Sale', 'Beverage Sale', 'TOBACCO SALE', 'LIQUOR SALE') THEN SIT.NetAmount ELSE 0 END) AS OtherSale
--INTO #CUR_SALE_TEMP 
--FROM Rista_SaleInvoices (NOLOCK) SI 
--INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
--INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
--WHERE	SI.InvoiceDay = @LW_DATE
--		AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
--GROUP BY SI.InvoiceDay

;WITH CTE_InvoiceData AS (
	SELECT SI.InvoiceDay, sum(SI.PersonCount) PersonCount, SUM(SI.NetChargeAmount) NetChargeAmount
	FROM Rista_SaleInvoices (NOLOCK) SI
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
	WHERE SI.InvoiceDay = @USERDATE AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
	group by InvoiceDay
)
SELECT	InvoiceDay, COVERS, (FoodSale + BeverageSale + LiquorSale + TobaccoSale + OtherSale)/COVERS APC,
	FoodSale, BeverageSale, LiquorSale, TobaccoSale, OtherSale, NetChargeAmount, (FoodSale + BeverageSale + LiquorSale + TobaccoSale + OtherSale + NetChargeAmount) TOTAL
INTO #CUR_SALE_TEMP
FROM (
	SELECT
		ID.InvoiceDay,
		MAX(ID.PersonCount) AS COVERS,
		MAX(ID.NetChargeAmount) AS NetChargeAmount,
		SUM(CASE WHEN SIT.AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) AS FoodSale,
		SUM(CASE WHEN SIT.AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) AS BeverageSale,
		SUM(CASE WHEN SIT.AccountName = 'LIQUOR SALE' THEN SIT.NetAmount ELSE 0 END) AS LiquorSale,
		SUM(CASE WHEN SIT.AccountName = 'TOBACCO SALE' THEN SIT.NetAmount ELSE 0 END) AS TobaccoSale,
		SUM(CASE WHEN SIT.AccountName NOT IN ('Food Sale', 'Beverage Sale', 'TOBACCO SALE', 'LIQUOR SALE') THEN SIT.NetAmount ELSE 0 END) AS OtherSale
	
	FROM CTE_InvoiceData ID  -- Using the subquery for correct person count
	INNER JOIN Rista_SaleInvoices SI ON ID.InvoiceDay = SI.InvoiceDay AND SI.InvoiceType <> 'NC' AND SI.[Status] <> 'Cancelled'
	INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
	GROUP BY ID.InvoiceDay
) TEMP_SALE;


;WITH CTE_InvoiceData AS (
	SELECT SI.InvoiceDay, sum(SI.PersonCount) PersonCount, SUM(SI.NetChargeAmount) NetChargeAmount
	FROM Rista_SaleInvoices (NOLOCK) SI
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
	WHERE SI.InvoiceDay = @LW_DATE AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
	group by InvoiceDay
)
SELECT	InvoiceDay, COVERS, (FoodSale + BeverageSale + LiquorSale + TobaccoSale + OtherSale)/COVERS APC,
	FoodSale, BeverageSale, LiquorSale, TobaccoSale, OtherSale, NetChargeAmount, (FoodSale + BeverageSale + LiquorSale + TobaccoSale + OtherSale + NetChargeAmount) TOTAL
INTO #LW_SALE_TEMP
FROM (
	SELECT
		ID.InvoiceDay,
		MAX(ID.PersonCount) AS COVERS,
		MAX(ID.NetChargeAmount) AS NetChargeAmount,
		SUM(CASE WHEN SIT.AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) AS FoodSale,
		SUM(CASE WHEN SIT.AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) AS BeverageSale,
		SUM(CASE WHEN SIT.AccountName = 'LIQUOR SALE' THEN SIT.NetAmount ELSE 0 END) AS LiquorSale,
		SUM(CASE WHEN SIT.AccountName = 'TOBACCO SALE' THEN SIT.NetAmount ELSE 0 END) AS TobaccoSale,
		SUM(CASE WHEN SIT.AccountName NOT IN ('Food Sale', 'Beverage Sale', 'TOBACCO SALE', 'LIQUOR SALE') THEN SIT.NetAmount ELSE 0 END) AS OtherSale
	
	FROM CTE_InvoiceData ID  -- Using the subquery for correct person count
	INNER JOIN Rista_SaleInvoices SI ON ID.InvoiceDay = SI.InvoiceDay AND SI.InvoiceType <> 'NC' AND SI.[Status] <> 'Cancelled'
	INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
	GROUP BY ID.InvoiceDay
) TEMP_SALE;

;WITH CTE_InvoiceData AS (
	SELECT SI.InvoiceDay, sum(SI.PersonCount) PersonCount, SUM(SI.NetChargeAmount) NetChargeAmount
	FROM Rista_SaleInvoices (NOLOCK) SI
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
	WHERE SI.InvoiceDay = @LY_DATE AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
	group by InvoiceDay
)
SELECT	InvoiceDay, COVERS, (FoodSale + BeverageSale + LiquorSale + TobaccoSale + OtherSale)/COVERS APC,
	FoodSale, BeverageSale, LiquorSale, TobaccoSale, OtherSale, NetChargeAmount, (FoodSale + BeverageSale + LiquorSale + TobaccoSale + OtherSale + NetChargeAmount) TOTAL
INTO #LY_SALE_TEMP
FROM (
	SELECT
		ID.InvoiceDay,
		MAX(ID.PersonCount) AS COVERS,
		MAX(ID.NetChargeAmount) AS NetChargeAmount,
		SUM(CASE WHEN SIT.AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) AS FoodSale,
		SUM(CASE WHEN SIT.AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) AS BeverageSale,
		SUM(CASE WHEN SIT.AccountName = 'LIQUOR SALE' THEN SIT.NetAmount ELSE 0 END) AS LiquorSale,
		SUM(CASE WHEN SIT.AccountName = 'TOBACCO SALE' THEN SIT.NetAmount ELSE 0 END) AS TobaccoSale,
		SUM(CASE WHEN SIT.AccountName NOT IN ('Food Sale', 'Beverage Sale', 'TOBACCO SALE', 'LIQUOR SALE') THEN SIT.NetAmount ELSE 0 END) AS OtherSale
	
	FROM CTE_InvoiceData ID  -- Using the subquery for correct person count
	INNER JOIN Rista_SaleInvoices SI ON ID.InvoiceDay = SI.InvoiceDay AND SI.InvoiceType <> 'NC' AND SI.[Status] <> 'Cancelled'
	INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
	GROUP BY ID.InvoiceDay
) TEMP_SALE;

SELECT * FROM #CUR_SALE_TEMP
SELECT * FROM #LW_SALE_TEMP
SELECT * FROM #LY_SALE_TEMP
