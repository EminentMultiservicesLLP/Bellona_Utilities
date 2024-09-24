
USE [BELLONA_LIVE]
GO
CREATE NONCLUSTERED INDEX INDX_InvoiceType_1
ON [dbo].[Rista_SaleInvoices] ([InvoiceType])
INCLUDE ([branchCode],[InvoiceDay],[NetDiscountAmount],[NetDirectChargeAmount],[NetAmount],[NetChargeAmount])
GO

CREATE NONCLUSTERED INDEX [INDX_InvoiceId]
ON [dbo].[Rista_SaleItems] ([InvoiceID])
INCLUDE ([AccountName],[NetAmount])

go

CREATE NONCLUSTERED INDEX INDX_InvoiceType
ON [dbo].[Rista_SaleInvoices] ([InvoiceType])
INCLUDE ([branchCode],[InvoiceDay])
Go


IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PAYMENT_AGGREGATOR' AND TABLE_SCHEMA = 'DBO')
BEGIN
  CREATE TABLE PAYMENT_AGGREGATOR
	(	ID INT IDENTITY(1,1) NOT NULL,
		AGGREGATOR VARCHAR(50),
		DEACTIVE INT DEFAULT 0,
		CREATEDDATE DATETIME DEFAULT CURRENT_TIMESTAMP
	)
END
ELSE
	PRINT 'TABLE "PAYMENT_AGGREGATOR" ALREADY EXIST IN DATABASE'

GO
IF NOT EXISTS (SELECT * FROM PAYMENT_AGGREGATOR WHERE AGGREGATOR IN ('BOOK MY SHOW', 'Dineout', 'Easy Diner', 'SWIGGY Dineout'))
BEGIN
	INSERT INTO PAYMENT_AGGREGATOR (AGGREGATOR)
	SELECT 'BOOK MY SHOW' UNION 
	SELECT 'Dineout' UNION 
	SELECT 'Easy Diner' UNION 
	SELECT 'SWIGGY Dineout' UNION 
	SELECT 'Zomato Pro'
END
ELSE
	PRINT 'ENTRIES IS ALREADY PRESENT IN "PAYMENT_AGGREGATOR" TABLE'
GO


CREATE or alter PROCEDURE dbo.dbsp_GetDSR_Summary
(	@Startdt datetime, @Enddt  datetime, @branchCode varchar(20) = null )
AS
BEGIN
	/* exec dbo.dbsp_GetDSR_Summary @Startdt = '2024-09-14',  @Enddt = '2024-09-14', @branchCode='bnmaisha' */
	/** Get Sale Main Category details 
		1.	Here we are taking Netamount of each Item category like Foodsale, Beverage sale...
		2.	total amount is Netamount + charges(service charge /packaging charge..)
		
		Note: There are few sales which are marked as NC (No Charge), we need to exclude those from total sale for food, beverage....
	*******/

	;WITH CTE_ITEM_SALE(FoodSale, BeverageSale, LiquorSale, TobaccoSale, OtherSale)
	AS (
		SELECT 	
			SUM(CASE WHEN AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) AS FoodSale,
			SUM(CASE WHEN AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) AS BeverageSale,
			SUM(CASE WHEN AccountName = 'LIQUOR SALE' THEN SIT.NetAmount ELSE 0 END) AS LiquorSale,
			SUM(CASE WHEN AccountName = 'TOBACCO SALE' THEN SIT.NetAmount ELSE 0 END) AS TobaccoSale,
			SUM(CASE WHEN AccountName NOT IN ('Food Sale', 'Beverage Sale', 'TOBACCO SALE', 'LIQUOR SALE') THEN SIT.NetAmount ELSE 0 END) AS OtherSale
		FROM Rista_SaleInvoices (NOLOCK) SI 
		INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
			AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
	),
	CTE_NETSALE AS 
	(
		SELECT	SUM(SI.NETAMOUNT) NetSale,
				SUM(SI.NetDiscountAmount) NetDiscountAmount,
				SUM(SI.NetChargeAmount) NetChargeAmount,
				SUM(SI.NetDirectChargeAmount) DirectCharge
		FROM Rista_SaleInvoices (NOLOCK) SI
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt  
			AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
	)
	SELECT	
			FoodSale, BeverageSale, LiquorSale, TobaccoSale, OtherSale,
			CNS.NetDiscountAmount AS DiscountAmount, 
			CNS.NetChargeAmount AS ServiceChargeAmount, 
			CNS.DirectCharge, 
			CNS.NetSale AS SalesNetTotal, 
			(CNS.NetSale + CNS.NetChargeAmount) AS SalesTotalWithSC
	FROM CTE_ITEM_SALE CIS
	LEFT JOIN CTE_NETSALE CNS ON 1 = 1;

	/** Delivery for Food Sale  and Beverage sale 
		For delivery there is no service charge, only packaging charge being added as 25rs as of now 18-09-2024
		So for delivery (all item net amount) + packaging charge
	***/
	SELECT	CASE WHEN SUM(A.DeliveryFoodSale) > 0 THEN SUM(A.DeliveryFoodSale) + SUM(B.NetDirectChargeAmount) ELSE 0 END AS DeliveryFoodSale,
			CASE WHEN SUM(A.DeliveryFoodSale) = 0 AND SUM(A.DeliveryBeverageSale) > 0 THEN SUM(A.DeliveryBeverageSale) + SUM(B.NetDirectChargeAmount) ELSE 0 END AS DeliveryBeverageSale
	FROM 
	(	SELECT si.InvoiceID,
			SUM(CASE WHEN AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) AS DeliveryFoodSale,
			SUM(CASE WHEN AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) AS DeliveryBeverageSale
		FROM Rista_SaleInvoices (NOLOCK) SI 
		INNER JOIN Rista_SaleSourceInfo (NOLOCK) SSI  ON SI.InvoiceID = SSI.InvoiceID AND SSI.IsEcomOrder = 1
		INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
			AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled'
		GROUP BY SI.InvoiceID
	) A INNER JOIN Rista_SaleInvoices (NOLOCK) B ON A.InvoiceID = B.INVOICEID;
	

	/** Dine-in Sale details category wise **/
	SELECT 	
		SUM(CASE WHEN AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) AS DineInFoodSale,
		SUM(CASE WHEN AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) AS DineInBeverageSale,
		SUM(CASE WHEN AccountName = 'LIQUOR SALE' THEN SIT.NetAmount ELSE 0 END) AS DineInLiquorSale,
		SUM(CASE WHEN AccountName = 'TOBACCO SALE' THEN SIT.NetAmount ELSE 0 END) AS DineInTobaccoSale,
		SUM(CASE WHEN AccountName NOT IN ('Food Sale', 'Beverage Sale', 'TOBACCO SALE', 'LIQUOR SALE') THEN SIT.NetAmount ELSE 0 END) AS DineInOtherSale
	FROM Rista_SaleInvoices (NOLOCK) SI 
	INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
	LEFT JOIN Rista_SaleSourceInfo (NOLOCK) SSI ON SI.InvoiceID = SSI.InvoiceID AND SSI.IsEcomOrder = 1
	WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
		AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
		AND SSI.InvoiceID IS NULL 
		AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled';

/*** Dine In Covers 
		1.	All delivery order from apps like Zomato, Swiggy.. are being captured in Rista_SaleSourceInfo table where IsEcomOrder field is 1
		2.	Cover is person count for each invoice, 
		3.	Rista summary UI considering each entries in Rista_SaleSourceInfo (which is for ECOM order) as 1 entry and hence if you check 
			InvoiceDay = '2024-09-14' AND branchCode = 'bnlpchcf' on rista summary you will get 123 person count where as in table dine-in person count is 120
			Rista Summary page adding 1 for each etry in  Rista_SaleSourceInfo + sum of person count in Rista_SaleInvoices for that day for that branch

			? how Rista_SaleSourceInfo linked with Rista_SaleDelivery,
				1. for InvoiceDay = '2024-09-14' AND branchCode = 'bnlpchcf', we have 3 entries for unique invoice id
					where has we have 7 entried in delivery table?
				2. while calculating "APC DINE IN" average per cover, do we need to include delivery count(which is 1 for each order) + total cover ?
	********/	
	;WITH CTE_APCDINEIN(TotalCovers, TotalSale)
	AS(
		SELECT	SUM(SI.PERSONCOUNT) TotalCovers, 
				SUM(SI.NETAMOUNT) TotalSale
		FROM Rista_SaleInvoices (NOLOCK) SI
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) SD  ON SI.InvoiceID = SD.InvoiceID
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
			AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled'
	)
	SELECT	
		TotalCovers AS DineInCovers,
		TotalSale / (TotalCovers) AS APCDineIn
	FROM CTE_APCDINEIN;

	/*** DELIVERY DETAILS WITH BILLS COUNT + SALE  -- NOT CONSIDERING TAKEAWAY 
		All delivery order being captured in [Rista_SaleSourceInfo] table where IsECOMorder flag is 1
	*/
	SELECT 	
		ISNULL(SUM(CASE WHEN Channel LIKE '%ZOMATO%' THEN 1 ELSE 0 END), 0) AS ZomatoDeliveryBillsNo,
		ISNULL(SUM(CASE WHEN Channel LIKE '%ZOMATO%' THEN SI.NetAmount ELSE 0 END), 0) AS ZomatoDeliverySale,
		ISNULL(SUM(CASE WHEN Channel LIKE '%SWIGGY%' THEN 1 ELSE 0 END), 0) AS SwiggyDeliveryBillsNo,
		ISNULL(SUM(CASE WHEN Channel LIKE '%SWIGGY%' THEN SI.NetAmount ELSE 0 END), 0) AS SwiggyDeliverySale,
		ISNULL(SUM(CASE WHEN Channel NOT LIKE '%ZOMATO%' AND Channel NOT LIKE '%SWIGGY%' THEN 1 ELSE 0 END), 0) AS DeliveryChannel3BillsNo,
		ISNULL(SUM(CASE WHEN Channel NOT LIKE '%ZOMATO%' AND Channel NOT LIKE '%SWIGGY%' THEN SI.NetAmount ELSE 0 END), 0) AS DeliveryChannel3Sale,
		COUNT(DISTINCT SI.InvoiceID) AS DeliveryBillsTotalNo,
		ISNULL(SUM(SI.NETAMOUNT), 0) AS DeliveryBillsAmountTotal
	FROM Rista_SaleInvoices (NOLOCK) SI 
	INNER JOIN [dbo].[Rista_SaleSourceInfo] (NOLOCK) SSI ON SI.InvoiceID = SSI.InvoiceID
	WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
		AND SSI.IsEcomOrder = 1
		AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled'
					
	/** Payment via partners 
		Table booking through Zomato/Swiggy is not supported in rista..means...   any table orders would be of In store dine in ...
		although through API, ( No Zomato/swiggy), table and reservation could booked.
		So here Business means to get information on payment done via multiple apps like Zomato Pro, DIneout, EasyDiner..

		Rista_SalePayments table maintains all payment mode with Total bill amount( note: not Net amount) 

		Here we pull Total payment made via third party not Net amount
		One Invoice can have multiple payment methods like some in cash, some via credit card and some via thirdparty (zomato pro...)
		Hence, considering below points we are pulling amount from Rista_salespayment table
			1.	FInance team use Total paid amount via thirdparty , not net amount
			2.	Payment can be split into multiple payment channel and invoice table do need keep such info
	****/
	;WITH SalesData_ZOMATO AS (
		SELECT
			ISNULL(SUM(rsp.Amount), 0) AS DINEINSALE,
			ISNULL(SUM(RSI.PERSONCOUNT), 0) AS DINEINCOVERS,
			COUNT(DISTINCT RSI.INVOICEID) AS DINEINBILLS
		FROM Rista_SaleInvoices (NOLOCK) rsi
		INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi.InvoiceID = rsp.InvoiceID
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi.InvoiceID = rssi.InvoiceID
		WHERE	rsi.InvoiceDay BETWEEN @Startdt and @Enddt  and rssi.InvoiceID IS NULL
				AND rsp.mode like '%Zomato%PRO%'  AND RSI.InvoiceType <> 'NC'   AND RSI.Status <> 'Cancelled'
				and rsi.branchCode = (CASE WHEN @branchCode IS NULL THEN rsi.branchCode ELSE @branchCode END)
	),
	SalesData_DINEOUT AS (
		SELECT
			ISNULL(SUM(rsp.Amount), 0) AS DINEINSALE,
			ISNULL(SUM(RSI.PERSONCOUNT), 0) AS DINEINCOVERS,
			COUNT(DISTINCT RSI.INVOICEID) AS DINEINBILLS
		FROM Rista_SaleInvoices (NOLOCK) rsi
		INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi.InvoiceID = rsp.InvoiceID
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi.InvoiceID = rssi.InvoiceID
		WHERE	rsi.InvoiceDay BETWEEN @Startdt and @Enddt  and rssi.InvoiceID IS NULL
				AND rsp.mode like '%Dine%Out%'  AND RSI.InvoiceType <> 'NC' AND RSI.Status <> 'Cancelled'
				and rsi.branchCode = (CASE WHEN @branchCode IS NULL THEN rsi.branchCode ELSE @branchCode END)
	),
	SalesData_EAZYDINER AS (
		SELECT
			ISNULL(SUM(rsp.Amount), 0) AS DINEINSALE,
			ISNULL(SUM(RSI.PERSONCOUNT), 0) AS DINEINCOVERS,
			COUNT(DISTINCT RSI.INVOICEID) AS DINEINBILLS
		FROM Rista_SaleInvoices (NOLOCK) rsi
		INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi.InvoiceID = rsp.InvoiceID
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi.InvoiceID = rssi.InvoiceID
		WHERE	rsi.InvoiceDay BETWEEN @Startdt and @Enddt  and rssi.InvoiceID IS NULL
				AND rsp.mode like '%EASY%DINER%'   AND RSI.InvoiceType <> 'NC'  AND RSI.Status <> 'Cancelled'
				and rsi.branchCode = (CASE WHEN @branchCode IS NULL THEN rsi.branchCode ELSE @branchCode END)
	),
	SalesData_OTHER AS (
		SELECT
			ISNULL(SUM(rsp.Amount), 0) AS DINEINSALE,
			ISNULL(SUM(RSI.PERSONCOUNT), 0) AS DINEINCOVERS,
			COUNT(DISTINCT RSI.INVOICEID) AS DINEINBILLS
		FROM Rista_SaleInvoices (NOLOCK) rsi
		INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi.InvoiceID = rsp.InvoiceID
		INNER JOIN PAYMENT_AGGREGATOR (NOLOCK) PA ON RSP.Mode = PA.AGGREGATOR
			AND PA.AGGREGATOR NOT LIKE '%Zomato%PRO%' AND PA.AGGREGATOR NOT LIKE '%EASY%DINER%' AND PA.AGGREGATOR NOT LIKE '%Dine%Out%'
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi.InvoiceID = rssi.InvoiceID
		WHERE	rsi.InvoiceDay BETWEEN @Startdt and @Enddt  
				and rssi.InvoiceID IS NULL 
				AND RSI.InvoiceType <> 'NC'  AND RSI.Status <> 'Cancelled'
				and rsi.branchCode = (CASE WHEN @branchCode IS NULL THEN rsi.branchCode ELSE @branchCode END)
	)
	-- Final Select Statement to produce single row data
	SELECT
		-- Zomato Data
		zomato.DINEINSALE AS ZOMATODINEINSALE,
		zomato.DINEINCOVERS AS ZOMATODINEINCOVERS,
		zomato.DINEINBILLS AS ZOMATODINEINBILLS,
		(zomato.DINEINSALE / (case zomato.DINEINBILLS when 0 then 1 else zomato.DINEINBILLS end)) AS AVGBILLAMOUNTZOMATO,

    
		-- Dineout Data
		dineout.DINEINSALE AS DINEOUTDINEINSALE,
		dineout.DINEINCOVERS AS DINEOUTDINEINCOVERS,
		dineout.DINEINBILLS AS DINEOUTDINEINBILLS,
		(dineout.DINEINSALE / (case dineout.DINEINBILLS when 0 then 1 else dineout.DINEINBILLS end)) AS AVGBILLAMOUNTDINEOUT,
    
		-- EazyDiner Data
		eazydiner.DINEINSALE AS EASYDINERDINEINSALE,
		eazydiner.DINEINCOVERS AS EASYDINERDINEINCOVERS,
		eazydiner.DINEINBILLS AS EASYDINERDINEINBILLS,
		(eazydiner.DINEINSALE / (case eazydiner.DINEINBILLS when 0 then 1 else eazydiner.DINEINBILLS end)) AS AVGBILLAMOUNTEASYDINER,

		-- Other Data (ALL - Zomato - Dineout - EasyDiner)
		other.DINEINSALE AS otherDINEINSALE,
		other.DINEINCOVERS AS otherDINEINCOVERS,
		other.DINEINBILLS AS otherDINEINBILLS,
		(other.DINEINSALE / (case other.DINEINBILLS when 0 then 1 else other.DINEINBILLS end)) AS AVGBILLAMOUNTOTHER

	FROM  SalesData_OTHER other
	LEFT JOIN SalesData_ZOMATO zomato ON 1 = 1
	LEFT JOIN SalesData_DINEOUT dineout ON 1 = 1
	LEFT JOIN SalesData_EAZYDINER eazydiner ON 1 = 1;

END
GO