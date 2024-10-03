DECLARE	@Startdt datetime, @Enddt  datetime, @branchCode varchar(20) = null 
SET @Startdt = '2024-09-14'
SET @Enddt = '2024-09-14'

	/* exec dbo.dbsp_GetDSR_Summary @Startdt = '2024-09-14',  @Enddt = '2024-09-14', @branchCode='bnmaisha' */
	/** Get Sale Main Category details 
		1.	Here we are taking Netamount of each Item category like Foodsale, Beverage sale...
		2.	total amount is Netamount + charges(service charge /packaging charge..)
		
		Note: There are few sales which are marked as NC (No Charge), we need to exclude those from total sale for food, beverage....
	*******/
	;WITH CTE_ITEM_SALE(branchCode, OutletName,FoodSale, BeverageSale, LiquorSale, TobaccoSale, OtherSale)
	AS (
		SELECT 	
			SI.branchCode, MO.OutletName,
			SUM(CASE WHEN AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) AS FoodSale,
			SUM(CASE WHEN AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) AS BeverageSale,
			SUM(CASE WHEN AccountName = 'LIQUOR SALE' THEN SIT.NetAmount ELSE 0 END) AS LiquorSale,
			SUM(CASE WHEN AccountName = 'TOBACCO SALE' THEN SIT.NetAmount ELSE 0 END) AS TobaccoSale,
			SUM(CASE WHEN AccountName NOT IN ('Food Sale', 'Beverage Sale', 'TOBACCO SALE', 'LIQUOR SALE') THEN SIT.NetAmount ELSE 0 END) AS OtherSale
		FROM Rista_SaleInvoices (NOLOCK) SI 
		INNER JOIN MST_OUTLET MO ON SI.branchCode = MO.OutletCode
		INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
			AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
		GROUP BY SI.branchCode, MO.OutletName
	),
	CTE_NETSALE AS 
	(
		SELECT	SI.branchCode, MO.OutletName,
				SUM(SI.NETAMOUNT) NetSale,
				SUM(SI.NetDiscountAmount) NetDiscountAmount,
				SUM(SI.NetChargeAmount) NetChargeAmount,
				SUM(SI.NetDirectChargeAmount) DirectCharge
		FROM Rista_SaleInvoices (NOLOCK) SI
		INNER JOIN MST_OUTLET MO ON SI.branchCode = MO.OutletCode
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt  
			AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
		GROUP BY SI.branchCode, MO.OutletName
	),
	CTE_DELIVERY AS
	(
		SELECT si.InvoiceID, SI.branchCode, MO.OutletName,
			SUM(CASE WHEN AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) AS DeliveryFoodSale,
			SUM(CASE WHEN AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) AS DeliveryBeverageSale
		FROM Rista_SaleInvoices (NOLOCK) SI
		INNER JOIN MST_OUTLET MO ON SI.branchCode = MO.OutletCode
		INNER JOIN Rista_SaleSourceInfo (NOLOCK) SSI  ON SI.InvoiceID = SSI.InvoiceID AND SSI.IsEcomOrder = 1
		INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
			AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled'
		GROUP BY SI.InvoiceID, SI.branchCode, MO.OutletName
	),
	CTE_SALECATEGORYWISE AS
	(
		/** Dine-in Sale details category wise **/
		SELECT 	
			SI.branchCode, MO.OutletName,
			SUM(CASE WHEN AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) DineIn_FoodSale_Net,
			SUM(CASE WHEN AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) DineIn_BeverageSale_Net,
			SUM(CASE WHEN AccountName = 'LIQUOR SALE' THEN SIT.NetAmount ELSE 0 END) DineIn_LiquorSale_Net,
			SUM(CASE WHEN AccountName = 'TOBACCO SALE' THEN SIT.NetAmount ELSE 0 END) DineIn_TobaccoSale_Net,
			SUM(CASE WHEN AccountName NOT IN ('Food Sale', 'Beverage Sale', 'TOBACCO SALE', 'LIQUOR SALE') THEN SIT.NetAmount ELSE 0 END) DineIn_OtherSale_Net
		FROM Rista_SaleInvoices (NOLOCK) SI
		INNER JOIN MST_OUTLET MO ON SI.branchCode = MO.OutletCode
		INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) SSI ON SI.InvoiceID = SSI.InvoiceID AND SSI.IsEcomOrder = 1
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
			AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			AND SSI.InvoiceID IS NULL 
			AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled' AND SI.Channel like 'Dine%'
		GROUP BY SI.branchCode, MO.OutletName
	),
	CTE_APCDINEIN
	AS(
		SELECT	SI.branchCode, MO.OUTLETNAME,
				SUM(SI.PERSONCOUNT) TotalCovers, 
				SUM(SI.NETAMOUNT) TotalSale
		FROM Rista_SaleInvoices (NOLOCK) SI
		INNER JOIN MST_OUTLET MO ON SI.branchCode = MO.OutletCode
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) SD  ON SI.InvoiceID = SD.InvoiceID
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
			AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled' AND SI.Channel like 'Dine%'
		GROUP BY SI.branchCode, MO.OUTLETNAME
	),
	CTE_DELIVERYDETAILS
	AS (
		SELECT 	
			SI.BRANCHCODE, MO.OUTLETNAME,
			ISNULL(SUM(CASE WHEN Channel LIKE '%ZOMATO%' THEN 1 ELSE 0 END), 0) ZomatoDeliveryBillsNo,
			ISNULL(SUM(CASE WHEN Channel LIKE '%ZOMATO%' THEN SI.NetAmount ELSE 0 END), 0) ZomatoDeliverySaleNet,
			ISNULL(SUM(CASE WHEN Channel LIKE '%SWIGGY%' THEN 1 ELSE 0 END), 0) SwiggyDeliveryBillsNo,
			ISNULL(SUM(CASE WHEN Channel LIKE '%SWIGGY%' THEN SI.NetAmount ELSE 0 END), 0) SwiggyDeliverySaleNet,
			ISNULL(SUM(CASE WHEN Channel NOT LIKE '%ZOMATO%' AND Channel NOT LIKE '%SWIGGY%' THEN 1 ELSE 0 END), 0) DeliveryChannel3BillsNo,
			ISNULL(SUM(CASE WHEN Channel NOT LIKE '%ZOMATO%' AND Channel NOT LIKE '%SWIGGY%' THEN SI.NetAmount ELSE 0 END), 0) DeliveryChannel3SaleNet,
			COUNT(DISTINCT SI.InvoiceID) DeliveryBillsTotalNo,
			ISNULL(SUM(SI.NETAMOUNT), 0) DeliveryBillsAmountTotal
		FROM Rista_SaleInvoices (NOLOCK) SI 
		INNER JOIN MST_OUTLET MO ON SI.branchCode = MO.OutletCode
		INNER JOIN [dbo].[Rista_SaleSourceInfo] (NOLOCK) SSI ON SI.InvoiceID = SSI.InvoiceID
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
			AND SSI.IsEcomOrder = 1
			AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
				AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled'
		GROUP BY SI.branchCode, MO.OUTLETNAME
	),
	SalesData_ZOMATO AS (
		SELECT
			rsi.BRANCHCODE, MO.OUTLETNAME,
			ISNULL(SUM(rsp.Amount), 0) AS DINEINSALE,
			ISNULL(SUM(RSI.PERSONCOUNT), 0) AS DINEINCOVERS,
			COUNT(DISTINCT RSI.INVOICEID) AS DINEINBILLS
		FROM Rista_SaleInvoices (NOLOCK) rsi
		INNER JOIN MST_OUTLET MO ON rsi.branchCode = MO.OutletCode
		INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi.InvoiceID = rsp.InvoiceID
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi.InvoiceID = rssi.InvoiceID
		WHERE	rsi.InvoiceDay BETWEEN @Startdt and @Enddt  and rssi.InvoiceID IS NULL
				AND rsp.mode like '%Zomato%PRO%'  AND RSI.InvoiceType <> 'NC'   AND RSI.Status <> 'Cancelled' 
				and rsi.branchCode = (CASE WHEN @branchCode IS NULL THEN rsi.branchCode ELSE @branchCode END)
		GROUP BY rsi.branchCode, MO.OUTLETNAME
	),
	SalesData_DINEOUT AS (
		SELECT
			rsi.BRANCHCODE, MO.OUTLETNAME,
			ISNULL(SUM(rsp.Amount), 0) AS DINEINSALE,
			ISNULL(SUM(RSI.PERSONCOUNT), 0) AS DINEINCOVERS,
			COUNT(DISTINCT RSI.INVOICEID) AS DINEINBILLS
		FROM Rista_SaleInvoices (NOLOCK) rsi
		INNER JOIN MST_OUTLET MO ON rsi.branchCode = MO.OutletCode
		INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi.InvoiceID = rsp.InvoiceID
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi.InvoiceID = rssi.InvoiceID
		WHERE	rsi.InvoiceDay BETWEEN @Startdt and @Enddt  and rssi.InvoiceID IS NULL
				AND rsp.mode like '%Dine%Out%'  AND RSI.InvoiceType <> 'NC' AND RSI.Status <> 'Cancelled'
				and rsi.branchCode = (CASE WHEN @branchCode IS NULL THEN rsi.branchCode ELSE @branchCode END)
		GROUP BY rsi.branchCode, MO.OUTLETNAME
	),
	SalesData_EAZYDINER AS (
		SELECT
			rsi.BRANCHCODE, MO.OUTLETNAME,
			ISNULL(SUM(rsp.Amount), 0) AS DINEINSALE,
			ISNULL(SUM(RSI.PERSONCOUNT), 0) AS DINEINCOVERS,
			COUNT(DISTINCT RSI.INVOICEID) AS DINEINBILLS
		FROM Rista_SaleInvoices (NOLOCK) rsi
		INNER JOIN MST_OUTLET MO ON rsi.branchCode = MO.OutletCode
		INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi.InvoiceID = rsp.InvoiceID
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi.InvoiceID = rssi.InvoiceID
		WHERE	rsi.InvoiceDay BETWEEN @Startdt and @Enddt  and rssi.InvoiceID IS NULL
				AND rsp.mode like '%EASY%DINER%'   AND RSI.InvoiceType <> 'NC'  AND RSI.Status <> 'Cancelled'
				and rsi.branchCode = (CASE WHEN @branchCode IS NULL THEN rsi.branchCode ELSE @branchCode END)
		GROUP BY rsi.branchCode, MO.OUTLETNAME
	),
	SalesData_OTHER AS (
		SELECT
			rsi.BRANCHCODE, MO.OUTLETNAME,
			ISNULL(SUM(rsp.Amount), 0) AS DINEINSALE,
			ISNULL(SUM(RSI.PERSONCOUNT), 0) AS DINEINCOVERS,
			COUNT(DISTINCT RSI.INVOICEID) AS DINEINBILLS
		FROM Rista_SaleInvoices (NOLOCK) rsi
		INNER JOIN MST_OUTLET MO ON rsi.branchCode = MO.OutletCode
		INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi.InvoiceID = rsp.InvoiceID
		INNER JOIN PAYMENT_AGGREGATOR (NOLOCK) PA ON RSP.Mode = PA.AGGREGATOR
			AND PA.AGGREGATOR NOT LIKE '%Zomato%PRO%' AND PA.AGGREGATOR NOT LIKE '%EASY%DINER%' AND PA.AGGREGATOR NOT LIKE '%Dine%Out%'
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi.InvoiceID = rssi.InvoiceID
		WHERE	rsi.InvoiceDay BETWEEN @Startdt and @Enddt  
				and rssi.InvoiceID IS NULL 
				AND RSI.InvoiceType <> 'NC'  AND RSI.Status <> 'Cancelled'
				and rsi.branchCode = (CASE WHEN @branchCode IS NULL THEN rsi.branchCode ELSE @branchCode END)
		GROUP BY rsi.branchCode, MO.OUTLETNAME
	)
	SELECT	cis.branchCode,cis.OutletName,
			SUM(FoodSale) FOODSALE_NET, SUM(BeverageSale) BEVERAGESALE_NET, SUM(LiquorSale) LIQUORSALE_NET, SUM(TobaccoSale) TOBACCOSALE_NET, 
			SUM(OtherSale) OTHERSALE_NET,  SUM(CNS.NetDiscountAmount) NETDISCOUNTAMOUNT, SUM(CNS.NetChargeAmount) NetChargeAmount, SUM(CNS.DirectCharge) DirectCharge, 
			SUM(CNS.NetSale) NetSale, SUM(CNS.NetSale + CNS.NetChargeAmount) SalesTotalWithSC,
			CASE WHEN SUM(CDL.DeliveryFoodSale) > 0 THEN SUM(CDL.DeliveryFoodSale) ELSE 0 END DeliveryFoodSaleNet,
			CASE WHEN SUM(CDL.DeliveryFoodSale) = 0 AND SUM(CDL.DeliveryBeverageSale) > 0 THEN SUM(CDL.DeliveryBeverageSale) ELSE 0 END DeliveryBeverageSaleNet,
			SUM(DineIn_FoodSale_Net) DineIn_FoodSale_Net, SUM(DineIn_BeverageSale_Net) DineIn_BeverageSale_Net, SUM(DineIn_LiquorSale_Net) DineIn_LiquorSale_Net,
			SUM(DineIn_TobaccoSale_Net) DineIn_TobaccoSale_Net,	SUM(DineIn_OtherSale_Net) DineIn_OtherSale_Net,
			SUM(TotalCovers) DineInCovers, SUM(TotalSale / (TotalCovers)),
			SUM(ZomatoDeliveryBillsNo) ZomatoDeliveryBillsNo, SUM(ZomatoDeliverySaleNet) ZomatoDeliverySaleNet,
			SUM(SwiggyDeliveryBillsNo) SwiggyDeliveryBillsNo, SUM(SwiggyDeliverySaleNet) SwiggyDeliverySaleNet,
			SUM(DeliveryChannel3BillsNo) DeliveryChannel3BillsNo, SUM(DeliveryChannel3SaleNet) DeliveryChannel3SaleNet,
			SUM(DeliveryBillsTotalNo) DeliveryBillsTotalNo, SUM(DeliveryBillsAmountTotal) DeliveryBillsAmountTotal,

			SUM(zomato.DINEINSALE) ZomatoDineInSaleNet, SUM(zomato.DINEINCOVERS) ZomatoDineInCovers, SUM(zomato.DINEINBILLS) ZomatoDineInBills,
			SUM(zomato.DINEINSALE / (case zomato.DINEINBILLS when 0 then 1 else zomato.DINEINBILLS end)) AvgBillAmountZomato,

		-- Dineout Data
			SUM(dineout.DINEINSALE) DineOutDineInSaleNet, SUM(dineout.DINEINCOVERS) DineOutDineInCovers,  SUM(dineout.DINEINBILLS) DineOutDineInBills,
			SUM(dineout.DINEINSALE / (case dineout.DINEINBILLS when 0 then 1 else dineout.DINEINBILLS end)) AvgBillAmountDineOut,
    
		-- EazyDiner Data
		  SUM(eazydiner.DINEINSALE) EazyDinerDineInSaleNet, SUM( eazydiner.DINEINCOVERS) EazyDinerDineInCovers,  SUM(eazydiner.DINEINBILLS) EazyDinerDineInBills,
		  SUM(eazydiner.DINEINSALE / (case eazydiner.DINEINBILLS when 0 then 1 else eazydiner.DINEINBILLS end)) AvgBillAmountEazyDiner,
	
		-- Other Data (ALL - Zomato - Dineout - EasyDiner)
		  SUM(other.DINEINSALE) OtherAggregatorDineInSaleNet,
		  SUM(other.DINEINCOVERS) OtherAggregatorDineInCovers,
		  SUM(other.DINEINBILLS) OtherAggregatorDineInBills,
		  SUM(other.DINEINSALE / (case other.DINEINBILLS when 0 then 1 else other.DINEINBILLS end)) AvgBillAmountOtherAggregator

	FROM CTE_ITEM_SALE CIS
	LEFT JOIN CTE_NETSALE CNS ON cis.branchCode = cns.branchCode
	LEFT JOIN CTE_DELIVERY CDL ON CIS.branchCode = CDL.branchCode
	LEFT JOIN CTE_SALECATEGORYWISE CSCW ON CIS.branchCode = CSCW.branchCode
	LEFT JOIN CTE_APCDINEIN CAD ON CIS.branchCode = CAD.branchCode
	LEFT JOIN CTE_DELIVERYDETAILS CDD ON CIS.BRANCHCODE = CDD.BRANCHCODE
	LEFT JOIN SalesData_OTHER other ON CIS.branchCode = other.branchCode
	LEFT JOIN SalesData_ZOMATO zomato ON CIS.branchCode = zomato.branchCode
	LEFT JOIN SalesData_DINEOUT dineout ON CIS.branchCode = dineout.branchCode
	LEFT JOIN SalesData_EAZYDINER eazydiner ON CIS.branchCode = eazydiner.branchCode

	GROUP BY cis.branchCode,cis.OutletName