USE [BELLONA_LIVE]
GO

/*
DECLARE @Startdt datetime, @Enddt  datetime, @branchCode varchar(20) = null, @clusterId int = 0
SET @Startdt ='2024-10-01'
SET @Enddt = '2024-10-01'  
SET @branchCode  = NULL
SET @clusterId  = 0

EXEC [dbo].[dbsp_GetDSR_Summary] @Startdt = @Startdt, @Enddt=@Enddt, @branchCode= @branchCode
EXEC [dbo].[dbsp_GetDSR_Summary_1] @Startdt = @Startdt, @Enddt=@Enddt, @branchCode= @branchCode
EXEC [dbo].[dbsp_GetDSR_Summary_2] @Startdt = @Startdt, @Enddt=@Enddt, @branchCode= @branchCode

select * from MST_Clusters
select * from [dbo].[MST_Cities]
*/
CREATE or ALTER   PROCEDURE [dbo].[dbsp_GetDSR_Summary]
(	@Startdt datetime, @Enddt  datetime, @branchCode varchar(20) = null, @clusterId int = 0, @cityId int = 0 )
AS
BEGIN
	IF @branchCode = '0' SET @branchCode = NULL
	DECLARE @OUTLETS TABLE(BRANCHCODE VARCHAR(20), BRANCHNAME VARCHAR(255), CLUSTERNAME VARCHAR(255), CityName VARCHAR(100))

	INSERT @OUTLETS (BRANCHCODE, BRANCHNAME, CLUSTERNAME, CityName)
	SELECT distinct OutletCode BRANCHCODE, OutletName BRANCHNAME, MC.ClusterName, MCT.CityName
	FROM MST_OUTLET MO 
	INNER JOIN MST_Clusters MC ON MO.ClusterID = MC.ClusterID
	INNER JOIN MST_Cities MCT ON MC.CityID = MCT.CityID
	WHERE	MC.CityID =  (CASE WHEN ISNULL(@cityId, 0) = 0 THEN MC.CityID ELSE @cityId END)
			AND MO.ClusterID = (CASE WHEN ISNULL(@clusterId, 0) = 0 THEN MO.ClusterID ELSE @clusterId END)
			AND OutletCode = (CASE WHEN ISNULL(@branchCode , '0') = '0' THEN OutletCode ELSE @branchCode END)  
	
	IF OBJECT_ID('tempdb..#CTE_ITEM_SALE_TEMP') IS NOT NULL DROP TABLE #CTE_ITEM_SALE_TEMP;
	IF OBJECT_ID('tempdb..#CTE_NETSALE_TEMP') IS NOT NULL DROP TABLE #CTE_NETSALE_TEMP;
	IF OBJECT_ID('tempdb..#CTE_DELIVERY_TEMP') IS NOT NULL DROP TABLE #CTE_DELIVERY_TEMP;
	IF OBJECT_ID('tempdb..#CTE_SALECATEGORYWISE_TEMP') IS NOT NULL DROP TABLE #CTE_SALECATEGORYWISE_TEMP;
	IF OBJECT_ID('tempdb..#CTE_APCDINEIN_TEMP') IS NOT NULL DROP TABLE #CTE_APCDINEIN_TEMP;
	IF OBJECT_ID('tempdb..#CTE_DELIVERYDETAILS_TEMP') IS NOT NULL DROP TABLE #CTE_DELIVERYDETAILS_TEMP;
	IF OBJECT_ID('tempdb..#SalesData_OTHER_TEMP') IS NOT NULL DROP TABLE #SalesData_OTHER_TEMP;
	IF OBJECT_ID('tempdb..#SalesData_ZOMATO_TEMP') IS NOT NULL DROP TABLE #SalesData_ZOMATO_TEMP;
	IF OBJECT_ID('tempdb..#SalesData_DINEOUT_TEMP') IS NOT NULL DROP TABLE #SalesData_DINEOUT_TEMP;
	IF OBJECT_ID('tempdb..#SalesData_EAZYDINER_TEMP') IS NOT NULL DROP TABLE #SalesData_EAZYDINER_TEMP;

	/* exec dbo.dbsp_GetDSR_Summary @Startdt = '2024-09-14',  @Enddt = '2024-09-14', @branchCode='bnmaisha' */
	/** Get Sale Main Category details 
		1.	Here we are taking Netamount of each Item category like Foodsale, Beverage sale...
		2.	total amount is Netamount + charges(service charge /packaging charge..)
		
		Note: There are few sales which are marked as NC (No Charge), we need to exclude those from total sale for food, beverage....
	*******/
	
	SELECT 	
		SI.branchCode,
		SUM(CASE WHEN AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) AS FoodSale,
		SUM(CASE WHEN AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) AS BeverageSale,
		SUM(CASE WHEN AccountName = 'LIQUOR SALE' THEN SIT.NetAmount ELSE 0 END) AS LiquorSale,
		SUM(CASE WHEN AccountName = 'TOBACCO SALE' THEN SIT.NetAmount ELSE 0 END) AS TobaccoSale,
		SUM(CASE WHEN AccountName NOT IN ('Food Sale', 'Beverage Sale', 'TOBACCO SALE', 'LIQUOR SALE') THEN SIT.NetAmount ELSE 0 END) AS OtherSale
	INTO #CTE_ITEM_SALE_TEMP 
	FROM Rista_SaleInvoices (NOLOCK) SI 
	INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
	WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
		--AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
		AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
	GROUP BY SI.branchCode
	
	
	SELECT	SI.branchCode,
			SUM(SI.NETAMOUNT) NetSale,
			SUM(SI.NetDiscountAmount) NetDiscountAmount,
			SUM(SI.NetChargeAmount) NetChargeAmount,
			SUM(SI.NetDirectChargeAmount) DirectCharge
	INTO #CTE_NETSALE_TEMP
	FROM Rista_SaleInvoices (NOLOCK) SI
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
	WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt  
		--AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
		AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
	GROUP BY SI.branchCode

	
	SELECT	SI.branchCode,
			SUM(CASE WHEN AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) AS DeliveryFoodSale,
			SUM(CASE WHEN AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) AS DeliveryBeverageSale
		INTO #CTE_DELIVERY_TEMP
		FROM Rista_SaleInvoices (NOLOCK) SI
		INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
		INNER JOIN Rista_SaleSourceInfo (NOLOCK) SSI  ON SI.InvoiceID = SSI.InvoiceID AND SSI.IsEcomOrder = 1
		INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
			--AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled'
		GROUP BY SI.branchCode
	
	
	SELECT 	
			SI.branchCode,
			SUM(CASE WHEN AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) DineIn_FoodSale_Net,
			SUM(CASE WHEN AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) DineIn_BeverageSale_Net,
			SUM(CASE WHEN AccountName = 'LIQUOR SALE' THEN SIT.NetAmount ELSE 0 END) DineIn_LiquorSale_Net,
			SUM(CASE WHEN AccountName = 'TOBACCO SALE' THEN SIT.NetAmount ELSE 0 END) DineIn_TobaccoSale_Net,
			SUM(CASE WHEN AccountName NOT IN ('Food Sale', 'Beverage Sale', 'TOBACCO SALE', 'LIQUOR SALE') THEN SIT.NetAmount ELSE 0 END) DineIn_OtherSale_Net
		INTO #CTE_SALECATEGORYWISE_TEMP
		FROM Rista_SaleInvoices (NOLOCK) SI
		INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
		INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) SSI ON SI.InvoiceID = SSI.InvoiceID AND SSI.IsEcomOrder = 1
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
			--AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			AND SSI.InvoiceID IS NULL 
			AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled' AND SI.Channel like 'Dine%'
		GROUP BY SI.branchCode
	
	
		SELECT	SI.branchCode,
				SUM(SI.PERSONCOUNT) TotalCovers, 
				SUM(SI.NETAMOUNT) TotalSale
		INTO #CTE_APCDINEIN_TEMP
		FROM Rista_SaleInvoices (NOLOCK) SI
		INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) SD  ON SI.InvoiceID = SD.InvoiceID
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
			--AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled' AND SI.Channel like 'Dine%'
		GROUP BY SI.branchCode
	

	SELECT 	
		SI.BRANCHCODE,
		ISNULL(SUM(CASE WHEN Channel LIKE '%ZOMATO%' THEN 1 ELSE 0 END), 0) ZomatoDeliveryBillsNo,
		ISNULL(SUM(CASE WHEN Channel LIKE '%ZOMATO%' THEN SI.NetAmount ELSE 0 END), 0) ZomatoDeliverySaleNet,
		ISNULL(SUM(CASE WHEN Channel LIKE '%SWIGGY%' THEN 1 ELSE 0 END), 0) SwiggyDeliveryBillsNo,
		ISNULL(SUM(CASE WHEN Channel LIKE '%SWIGGY%' THEN SI.NetAmount ELSE 0 END), 0) SwiggyDeliverySaleNet,
		ISNULL(SUM(CASE WHEN Channel NOT LIKE '%ZOMATO%' AND Channel NOT LIKE '%SWIGGY%' THEN 1 ELSE 0 END), 0) DeliveryChannel3BillsNo,
		ISNULL(SUM(CASE WHEN Channel NOT LIKE '%ZOMATO%' AND Channel NOT LIKE '%SWIGGY%' THEN SI.NetAmount ELSE 0 END), 0) DeliveryChannel3SaleNet,
		COUNT(DISTINCT SI.InvoiceID) DeliveryBillsTotalNo,
		ISNULL(SUM(SI.NETAMOUNT), 0) DeliveryBillsAmountTotal
	INTO #CTE_DELIVERYDETAILS_TEMP
	FROM Rista_SaleInvoices (NOLOCK) SI 
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
	INNER JOIN [dbo].[Rista_SaleSourceInfo] (NOLOCK) SSI ON SI.InvoiceID = SSI.InvoiceID
	WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
		AND SSI.IsEcomOrder = 1
		--AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
		AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled'
	GROUP BY SI.branchCode
	
	
	SELECT
		rsi.BRANCHCODE,
		ISNULL(SUM(rsp.Amount), 0) AS DINEINSALE,
		ISNULL(SUM(RSI.PERSONCOUNT), 0) AS DINEINCOVERS,
		COUNT(DISTINCT RSI.INVOICEID) AS DINEINBILLS
	INTO #SalesData_ZOMATO_TEMP
	FROM Rista_SaleInvoices (NOLOCK) rsi
	INNER JOIN @OUTLETS MO ON RSI.branchCode = MO.BRANCHCODE
	INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi.InvoiceID = rsp.InvoiceID
	LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi.InvoiceID = rssi.InvoiceID
	WHERE	rsi.InvoiceDay BETWEEN @Startdt and @Enddt  and rssi.InvoiceID IS NULL
			AND rsp.mode like '%Zomato%PRO%'  AND RSI.InvoiceType <> 'NC'   AND RSI.Status <> 'Cancelled' 
			--and rsi.branchCode = (CASE WHEN @branchCode IS NULL THEN rsi.branchCode ELSE @branchCode END)
	GROUP BY rsi.branchCode
	
	
	SELECT
		rsi.BRANCHCODE,
		ISNULL(SUM(rsp.Amount), 0) AS DINEINSALE,
		ISNULL(SUM(RSI.PERSONCOUNT), 0) AS DINEINCOVERS,
		COUNT(DISTINCT RSI.INVOICEID) AS DINEINBILLS
	INTO #SalesData_DINEOUT_TEMP
	FROM Rista_SaleInvoices (NOLOCK) rsi
	INNER JOIN @OUTLETS MO ON RSI.branchCode = MO.BRANCHCODE
	INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi.InvoiceID = rsp.InvoiceID
	LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi.InvoiceID = rssi.InvoiceID
	WHERE	rsi.InvoiceDay BETWEEN @Startdt and @Enddt  and rssi.InvoiceID IS NULL
			AND rsp.mode like '%Dine%Out%'  AND RSI.InvoiceType <> 'NC' AND RSI.Status <> 'Cancelled'
			--and rsi.branchCode = (CASE WHEN @branchCode IS NULL THEN rsi.branchCode ELSE @branchCode END)
	GROUP BY rsi.branchCode
	
	
	SELECT
		rsi.BRANCHCODE,
		ISNULL(SUM(rsp.Amount), 0) AS DINEINSALE,
		ISNULL(SUM(RSI.PERSONCOUNT), 0) AS DINEINCOVERS,
		COUNT(DISTINCT RSI.INVOICEID) AS DINEINBILLS
	INTO #SalesData_EAZYDINER_TEMP
	FROM Rista_SaleInvoices (NOLOCK) rsi
	INNER JOIN @OUTLETS MO ON RSI.branchCode = MO.BRANCHCODE
	INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi.InvoiceID = rsp.InvoiceID
	LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi.InvoiceID = rssi.InvoiceID
	WHERE	rsi.InvoiceDay BETWEEN @Startdt and @Enddt  and rssi.InvoiceID IS NULL
			AND rsp.mode like '%EASY%DINER%'   AND RSI.InvoiceType <> 'NC'  AND RSI.Status <> 'Cancelled'
			--and rsi.branchCode = (CASE WHEN @branchCode IS NULL THEN rsi.branchCode ELSE @branchCode END)
	GROUP BY rsi.branchCode
	
	
	SELECT
		rsi.BRANCHCODE,
		ISNULL(SUM(rsp.Amount), 0) AS DINEINSALE,
		ISNULL(SUM(RSI.PERSONCOUNT), 0) AS DINEINCOVERS,
		COUNT(DISTINCT RSI.INVOICEID) AS DINEINBILLS
	INTO #SalesData_OTHER_TEMP
	FROM Rista_SaleInvoices (NOLOCK) rsi
	INNER JOIN @OUTLETS MO ON RSI.branchCode = MO.BRANCHCODE
	INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi.InvoiceID = rsp.InvoiceID
	INNER JOIN PAYMENT_AGGREGATOR (NOLOCK) PA ON RSP.Mode = PA.AGGREGATOR
		AND PA.AGGREGATOR NOT LIKE '%Zomato%PRO%' AND PA.AGGREGATOR NOT LIKE '%EASY%DINER%' AND PA.AGGREGATOR NOT LIKE '%Dine%Out%'
	LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi.InvoiceID = rssi.InvoiceID
	WHERE	rsi.InvoiceDay BETWEEN @Startdt and @Enddt  
			and rssi.InvoiceID IS NULL 
			AND RSI.InvoiceType <> 'NC'  AND RSI.Status <> 'Cancelled'
			--and rsi.branchCode = (CASE WHEN @branchCode IS NULL THEN rsi.branchCode ELSE @branchCode END)
	GROUP BY rsi.branchCode
	
	
	SELECT	DISTINCT cis.branchCode,MO.BRANCHNAME, MO.CLUSTERNAME, MO.CityName, 
			ISNULL(SUM(CIS.FoodSale) ,0) FoodSaleNet, ISNULL(SUM(CIS.BeverageSale) ,0) BeverageSaleNet, ISNULL(SUM(CIS.LiquorSale) ,0) LiquorSaleNet, ISNULL(SUM(CIS.TobaccoSale) ,0) TobaccoSaleNet, 
			ISNULL(SUM(CIS.OtherSale) ,0) OtherSale1Net,  ISNULL(SUM(CNS.NetDiscountAmount) ,0) DiscountAmount, ISNULL(SUM(CNS.NetChargeAmount) ,0) ServiceChargeAmount, ISNULL(SUM(CNS.DirectCharge) ,0) DirectCharge, 
			ISNULL(SUM(CNS.NetSale) ,0) SalesNetTotal, ISNULL(SUM(CNS.NetSale + CNS.NetChargeAmount) ,0) SalesTotalWithSC,
			CASE WHEN ISNULL(SUM(CDL.DeliveryFoodSale) ,0) > 0 THEN ISNULL(SUM(CDL.DeliveryFoodSale) ,0) ELSE 0 END DeliveryFoodSaleNet,
			CASE WHEN ISNULL(SUM(CDL.DeliveryFoodSale) ,0) = 0 AND ISNULL(SUM(CDL.DeliveryBeverageSale) ,0) > 0 THEN ISNULL(SUM(CDL.DeliveryBeverageSale) ,0) ELSE 0 END DeliveryBeverageSaleNet,
			ISNULL(SUM(DineIn_FoodSale_Net) ,0) DineInFoodSaleNet, ISNULL(SUM(DineIn_BeverageSale_Net) ,0) DineInBeverageSaleNet, ISNULL(SUM(DineIn_LiquorSale_Net) ,0) DineInLiquorSaleNet,
			ISNULL(SUM(DineIn_TobaccoSale_Net) ,0) DineInTobaccoSaleNet,	ISNULL(SUM(DineIn_OtherSale_Net) ,0) DineInOtherSaleNet,
			ISNULL(SUM(TotalCovers) ,0) DineInCovers, ISNULL(SUM(TotalSale / (TotalCovers)), 0) ApcDineIn,
			ISNULL(SUM(ZomatoDeliveryBillsNo) ,0) ZomatoDeliveryBillsNo, ISNULL(SUM(ZomatoDeliverySaleNet) ,0) ZomatoDeliverySaleNet,
			ISNULL(SUM(SwiggyDeliveryBillsNo) ,0) SwiggyDeliveryBillsNo, ISNULL(SUM(SwiggyDeliverySaleNet) ,0) SwiggyDeliverySaleNet,
			ISNULL(SUM(DeliveryChannel3BillsNo) ,0) DeliveryChannel3BillsNo, ISNULL(SUM(DeliveryChannel3SaleNet) ,0) DeliveryChannel3SaleNet,
			ISNULL(SUM(DeliveryBillsTotalNo) ,0) DeliveryBillsTotalNo, ISNULL(SUM(DeliveryBillsAmountTotal) ,0) DeliveryBillsAmountTotal,
			--ZOomato
			ISNULL(SUM(zomato.DINEINSALE) ,0) ZomatoDineInSaleNet, ISNULL(SUM(zomato.DINEINCOVERS) ,0) ZomatoDineInCovers, ISNULL(SUM(zomato.DINEINBILLS) ,0) ZomatoDineInBills,
			ISNULL(SUM(zomato.DINEINSALE / (case zomato.DINEINBILLS when 0 then 1 else zomato.DINEINBILLS end)) ,0) AvgBillAmountZomato,

			-- Dineout Data
			ISNULL(SUM(dineout.DINEINSALE) ,0) DineOutDineInSaleNet, ISNULL(SUM(dineout.DINEINCOVERS) ,0) DineOutDineInCovers,  ISNULL(SUM(dineout.DINEINBILLS) ,0) DineOutDineInBills,
			ISNULL(SUM(dineout.DINEINSALE / (case dineout.DINEINBILLS when 0 then 1 else dineout.DINEINBILLS end)) ,0) AvgBillAmountDineOut,
    
			-- EazyDiner Data
			ISNULL(SUM(eazydiner.DINEINSALE) ,0) EazyDinerDineInSaleNet, ISNULL(SUM( eazydiner.DINEINCOVERS) ,0) EazyDinerDineInCovers,  ISNULL(SUM(eazydiner.DINEINBILLS) ,0) EazyDinerDineInBills,
			ISNULL(SUM(eazydiner.DINEINSALE / (case eazydiner.DINEINBILLS when 0 then 1 else eazydiner.DINEINBILLS end)) ,0) AvgBillAmountEazyDiner,
	
			-- Other Data (ALL - Zomato - Dineout - EasyDiner)
			ISNULL(SUM(other.DINEINSALE) ,0) OtherAggregatorDineInSaleNet, ISNULL(SUM(other.DINEINCOVERS) ,0) OtherAggregatorDineInCovers, ISNULL(SUM(other.DINEINBILLS) ,0) OtherAggregatorDineInBills, 
			ISNULL(SUM(other.DINEINSALE / (case other.DINEINBILLS when 0 then 1 else other.DINEINBILLS end)) ,0) AvgBillAmountOtherAggregator

	FROM #CTE_ITEM_SALE_TEMP CIS
	LEFT JOIN #CTE_NETSALE_TEMP CNS ON cis.branchCode = cns.branchCode
	LEFT JOIN #CTE_DELIVERY_TEMP CDL ON CIS.branchCode = CDL.branchCode
	LEFT JOIN #CTE_SALECATEGORYWISE_TEMP CSCW ON CIS.branchCode = CSCW.branchCode
	LEFT JOIN #CTE_APCDINEIN_TEMP CAD ON CIS.branchCode = CAD.branchCode
	LEFT JOIN #CTE_DELIVERYDETAILS_TEMP CDD ON CIS.BRANCHCODE = CDD.BRANCHCODE
	LEFT JOIN #SalesData_OTHER_TEMP other ON CIS.branchCode = other.branchCode
	LEFT JOIN #SalesData_ZOMATO_TEMP zomato ON CIS.branchCode = zomato.branchCode
	LEFT JOIN #SalesData_DINEOUT_TEMP dineout ON CIS.branchCode = dineout.branchCode
	LEFT JOIN #SalesData_EAZYDINER_TEMP eazydiner ON CIS.branchCode = eazydiner.branchCode
	LEFT JOIN @OUTLETS MO ON CIS.branchCode = MO.BRANCHCODE
	GROUP BY cis.branchCode,MO.BRANCHNAME, MO.CLUSTERNAME, MO.CityName
END
GO