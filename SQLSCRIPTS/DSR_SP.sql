--exec dbo.dbsp_GetDSR_Summary @Startdt = '2024-09-14',  @Enddt = '2024-09-14', @branchCode='bnmaisha'

CREATE or alter PROCEDURE dbo.dbsp_GetDSR_Summary
(	@Startdt datetime, @Enddt  datetime, @branchCode varchar(20) = null )
AS
BEGIN
	/** Get Sale Main Category details 
		1.	Here we are taking Netamount of each Item category like Foodsale, Beverage sale...
		2.	total amount is Netamount + charges(service charge /packaging charge..)
			
	*******/
	;WITH CTE_ITEM_SALE(FOODSALE,BEVERAGESALE,LIQUORSALE, TOBACOSALE,OTHERSALE)
	AS (
		SELECT 	
			SUM(CASE WHEN AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) AS 'FOOD SALE',
			SUM(CASE WHEN AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) AS 'BEVERAGE SALE',
			SUM(CASE WHEN AccountName = 'LIQUOR SALE' THEN SIT.NetAmount ELSE 0 END) AS 'LIQUOR SALE',
			SUM(CASE WHEN AccountName = 'TOBACO SALE' THEN SIT.NetAmount ELSE 0 END) AS 'TOBACO SALE',
			SUM(CASE WHEN AccountName NOT IN ('Food Sale', 'Beverage Sale', 'TOBACO SALE', 'LIQUOR SALE') THEN SIT.NetAmount ELSE 0 END) AS 'OTHER SALE'
		FROM Rista_SaleInvoices (NOLOCK) SI INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt and SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			  AND SI.InvoiceType <> 'NC'
		--WHERE SI.InvoiceDay = '2024-09-14' AND branchCode = 'bnlpchcf'
	),
	CTE_NETSALE AS 
	(
		SELECT	SUM(SI.NETAMOUNT) NETSALE,
				SUM(SI.NetDiscountAmount) NETDISCOUNTAMOUNT,
				SUM(SI.NetChargeAmount) NETCHARGEAMOUNT
				/*,
				--SUM(SI.TotalAmount) AS TOTALSALEWITHSC,
				SUM(SI.GrossAmount) AS TOTALGROSS */
		FROM Rista_SaleInvoices (NOLOCK) SI
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt  and SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
				 AND SI.InvoiceType <> 'NC'
		--WHERE SI.InvoiceDay = '2024-09-14' AND branchCode = 'bnlpchcf'	
	)
	SELECT	FOODSALE 'FOOD SALE',BEVERAGESALE 'BEVERAGE SALE',LIQUORSALE 'LIQUOR SALE', TOBACOSALE 'TOBACO SALE',OTHERSALE 'OTHER SALE',
			CNS.NETDISCOUNTAMOUNT 'DISCOUNT AMOUNT', CNS.NETCHARGEAMOUNT 'SERVICE CHARGE AMOUNT', CNS.NETSALE 'SALES NET TOTAL', 
			(CNS.NETSALE + cns.NETCHARGEAMOUNT) 'SALES TOTAL WITH SC'
	FROM CTE_ITEM_SALE CIS
	LEFT JOIN CTE_NETSALE CNS ON 1 = 1;

	/** Delivery for Food Sale  and Beverage sale ***/
	SELECT 	
		SUM(CASE WHEN AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) AS 'DELIVERY FOOD SALE',
		SUM(CASE WHEN AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) AS 'DELIVERY BEVERAGE SALE'
	FROM Rista_SaleInvoices (NOLOCK) SI 
	INNER JOIN Rista_SaleSourceInfo (NOLOCK) SSI  ON SI.InvoiceID = SSI.InvoiceID AND SSI.IsEcomOrder = 1
	INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
	WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt and SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			AND SI.InvoiceType <> 'NC'
	--WHERE SI.InvoiceDay = '2024-09-14' AND branchCode = 'bnlpchcf'
	
	/** Dine-in Sale details category wise ****/
	SELECT 	
		SUM(CASE WHEN AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) AS 'DINE IN FOOD SALE',
		SUM(CASE WHEN AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) AS 'DINE IN BEVERAGE SALE',
		SUM(CASE WHEN AccountName = 'LIQUOR SALE' THEN SIT.NetAmount ELSE 0 END) AS 'DINE IN LIQUOR SALE',
		SUM(CASE WHEN AccountName = 'TOBACO SALE' THEN SIT.NetAmount ELSE 0 END) AS 'DINE IN TOBACO SALE',
		SUM(CASE WHEN AccountName NOT IN ('Food Sale', 'Beverage Sale', 'TOBACO SALE', 'LIQUOR SALE') THEN SIT.NetAmount ELSE 0 END) AS 'DINE IN OTHER SALE'
	FROM Rista_SaleInvoices (NOLOCK) SI INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
	LEFT JOIN Rista_SaleSourceInfo (NOLOCK) SSI  ON SI.InvoiceID = SSI.InvoiceID AND SSI.IsEcomOrder = 1
	WHERE	SI.InvoiceDay BETWEEN @Startdt and @Enddt and SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			AND SSI.InvoiceID IS NULL AND SI.InvoiceType <> 'NC'


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
	;WITH CTE_APCDINEIN(TOTALCOVERS, TotalEcomOrder, TOTALSALE)
	AS(
		SELECT	SUM(SI.PERSONCOUNT) TOTALCOVERS, 
				COUNT(DISTINCT SD.INVOICEID)  TotalEcomOrder,
				SUM(SI.NETAMOUNT) TOTALSALE
		FROM	Rista_SaleInvoices (NOLOCK) SI
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) SD  ON SI.InvoiceID = SD.InvoiceID
		WHERE	SI.InvoiceDay BETWEEN @Startdt and @Enddt /*AND SD.InvoiceID IS NULL AND SI.PERSONCOUNT > 0 */
				 and SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
				  AND SI.InvoiceType <> 'NC'
		--WHERE SI.InvoiceDay = '2024-09-14' AND branchCode = 'bnlpchcf' AND SI.PERSONCOUNT > 0
	)
	SELECT	
			TOTALCOVERS 'DINE IN COVERS',
			TOTALSALE / (TOTALCOVERS) 'APC DINE IN'
	FROM CTE_APCDINEIN
	

	/*** DELIVERY DETAILS WITH BILLS COUNT + SALE  -- NOT CONSIDERING TAKEAWAY 
		All delivery order being captured in [Rista_SaleSourceInfo] table where IsECOMorder flag is 1
	*/
	SELECT 	
		SUM(CASE WHEN Channel LIKE '%ZOMATO%' THEN 1 ELSE 0 END) AS 'ZOMATO DELIVERY BILLS NO',
		SUM(CASE WHEN Channel LIKE '%ZOMATO%' THEN SI.NetAmount ELSE 0 END) AS 'ZOMATO DELIVERY SALE',
		SUM(CASE WHEN Channel LIKE '%SWIGGY%' THEN 1 ELSE 0 END) AS 'SWIGGY DELIVERY BILLS NO',
		SUM(CASE WHEN Channel LIKE '%SWIGGY%' THEN SI.NetAmount ELSE 0 END) AS 'SWIGGY DELIVERY SALE',
		SUM(CASE WHEN Channel NOT LIKE '%ZOMATO%' AND Channel NOT LIKE '%SWIGGY%' THEN 1 ELSE 0 END) AS 'DELIVERY CHANNEL 3 BILLS NO',
		SUM(CASE WHEN Channel NOT LIKE '%ZOMATO%' AND Channel NOT LIKE '%SWIGGY%' THEN SI.NetAmount ELSE 0 END) AS 'DELIVERY CHANNEL 3 SALE',
		COUNT(DISTINCT SI.InvoiceID) 'DELIVERY BILLS TOTAL NO',
		SUM(SI.NETAMOUNT) 'DELIVERY BILLS AMOUNT TOTAL'
	FROM Rista_SaleInvoices (NOLOCK) SI 
	--IF TAKEAWAY NEED TO CONSIDER THEN ISE LEFT JOIN BELOW
	INNER JOIN [dbo].[Rista_SaleSourceInfo] (NOLOCK) SSI ON SI.InvoiceID = SSI.InvoiceID
	--INNER JOIN DBO.Rista_SaleDelivery (NOLOCK) SD ON SI.InvoiceID = SD.InvoiceID
	WHERE	SI.InvoiceDay BETWEEN @Startdt and @Enddt AND SSI.ISECOMORDER = 1
			 and SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			  AND SI.InvoiceType <> 'NC'
	--WHERE SI.InvoiceDay = '2024-09-14' AND branchCode = 'bnlpchcf'
	

	/** Payment via partners 
		Table booking through Zomato/Swiggy is not supported in rista..means...   any table orders would be of In store dine in ...
		although through API, ( No Zomato/swiggy), table and reservation could booked.
		So here Business means to get information on payment done via multiple apps like Zomato Pro, DIneout, EasyDiner..

		Rista_SalePayments table maintains all payment mode with Total bill amount( note: not Net amount) 
	****/
	;WITH SalesData_ZOMATO AS (
		SELECT
			ISNULL(SUM(RSI.TotalAmount), 0) AS DINEINSALE,
			ISNULL(SUM(RSI.PERSONCOUNT), 0) AS DINEINCOVERS,
			COUNT(DISTINCT RSI.INVOICEID) AS DINEINBILLS
		FROM Rista_SaleInvoices (NOLOCK) rsi
		INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi.InvoiceID = rsp.InvoiceID
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi.InvoiceID = rssi.InvoiceID
		WHERE	rsi.InvoiceDay BETWEEN @Startdt and @Enddt  and rssi.InvoiceID IS NULL
				AND rsp.mode like '%Zomato%'  AND RSI.InvoiceType <> 'NC'
				and rsi.branchCode = (CASE WHEN @branchCode IS NULL THEN rsi.branchCode ELSE @branchCode END)
	),
	SalesData_DINEOUT AS (
		SELECT
			ISNULL(SUM(RSI.TotalAmount), 0) AS DINEINSALE,
			ISNULL(SUM(RSI.PERSONCOUNT), 0) AS DINEINCOVERS,
			COUNT(DISTINCT RSI.INVOICEID) AS DINEINBILLS
		FROM Rista_SaleInvoices (NOLOCK) rsi
		INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi.InvoiceID = rsp.InvoiceID
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi.InvoiceID = rssi.InvoiceID
		WHERE	rsi.InvoiceDay BETWEEN @Startdt and @Enddt  and rssi.InvoiceID IS NULL
				AND rsp.mode like '%Dine%Out%'  AND RSI.InvoiceType <> 'NC'
				and rsi.branchCode = (CASE WHEN @branchCode IS NULL THEN rsi.branchCode ELSE @branchCode END)
	),
	SalesData_EAZYDINER AS (
		SELECT
			ISNULL(SUM(RSI.TotalAmount), 0) AS DINEINSALE,
			ISNULL(SUM(RSI.PERSONCOUNT), 0) AS DINEINCOVERS,
			COUNT(DISTINCT RSI.INVOICEID) AS DINEINBILLS
		FROM Rista_SaleInvoices (NOLOCK) rsi
		INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi.InvoiceID = rsp.InvoiceID
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi.InvoiceID = rssi.InvoiceID
		WHERE	rsi.InvoiceDay BETWEEN @Startdt and @Enddt  and rssi.InvoiceID IS NULL
				AND rsp.mode like '%EASY%DINER%'   AND RSI.InvoiceType <> 'NC'
				and rsi.branchCode = (CASE WHEN @branchCode IS NULL THEN rsi.branchCode ELSE @branchCode END)
	),
	SalesData_ALL AS (
		SELECT
			ISNULL(SUM(RSI.TotalAmount), 0) AS DINEINSALE,
			ISNULL(SUM(RSI.PERSONCOUNT), 0) AS DINEINCOVERS,
			COUNT(DISTINCT RSI.INVOICEID) AS DINEINBILLS
		FROM Rista_SaleInvoices (NOLOCK) rsi
		INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi.InvoiceID = rsp.InvoiceID
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi.InvoiceID = rssi.InvoiceID
		WHERE	rsi.InvoiceDay BETWEEN @Startdt and @Enddt  and rssi.InvoiceID IS NULL   AND RSI.InvoiceType <> 'NC'
				and rsi.branchCode = (CASE WHEN @branchCode IS NULL THEN rsi.branchCode ELSE @branchCode END)
	)
	-- Final Select Statement to produce single row data
	SELECT
		-- Zomato Data
		zomato.DINEINSALE AS ZOMATO_DINEIN_SALE,
		zomato.DINEINCOVERS AS ZOMATO_DINEIN_COVERS,
		zomato.DINEINBILLS AS ZOMATO_DINEIN_BILLS,
    
		-- Dineout Data
		dineout.DINEINSALE AS DINEOUT_DINEIN_SALE,
		dineout.DINEINCOVERS AS DINEOUT_DINEIN_COVERS,
		dineout.DINEINBILLS AS DINEOUT_DINEIN_BILLS,
    
		-- EazyDiner Data
		eazydiner.DINEINSALE AS EASYDINER_DINEIN_SALE,
		eazydiner.DINEINCOVERS AS EASYDINER_DINEIN_COVERS,
		eazydiner.DINEINBILLS AS EASYDINER_DINEIN_BILLS,

		-- Other Data (ALL - Zomato - Dineout - EasyDiner)
		(all_data.DINEINSALE 
		 - ISNULL(zomato.DINEINSALE, 0) 
		 - ISNULL(dineout.DINEINSALE, 0) 
		 - ISNULL(eazydiner.DINEINSALE, 0)) AS OTHERS_DINEIN_SALE,

		(all_data.DINEINCOVERS 
		 - ISNULL(zomato.DINEINCOVERS, 0) 
		 - ISNULL(dineout.DINEINCOVERS, 0) 
		 - ISNULL(eazydiner.DINEINCOVERS, 0)) AS OTHERS_DINEIN_COVERS,

		(all_data.DINEINBILLS 
		 - ISNULL(zomato.DINEINBILLS, 0) 
		 - ISNULL(dineout.DINEINBILLS, 0) 
		 - ISNULL(eazydiner.DINEINBILLS, 0)) AS OTHERS_DINEIN_BILLS

	FROM  SalesData_ALL all_data
	LEFT JOIN SalesData_ZOMATO zomato ON 1 = 1
	LEFT JOIN SalesData_DINEOUT dineout ON 1 = 1
	LEFT JOIN SalesData_EAZYDINER eazydiner ON 1 = 1;

END
GO