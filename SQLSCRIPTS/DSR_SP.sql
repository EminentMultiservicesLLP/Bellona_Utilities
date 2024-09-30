
USE [BELLONA_LIVE]
GO

IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='Payment_Invoice_INDX' AND object_id = OBJECT_ID('[dbo].[Rista_SalePayments]'))
BEGIN
	CREATE NONCLUSTERED INDEX [Payment_Invoice_INDX]
	ON [dbo].[Rista_SalePayments] ([InvoiceID])
END
GO

IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='INDX_InvoiceType_1' AND object_id = OBJECT_ID('[dbo].[Rista_SaleInvoices]'))
BEGIN
	CREATE NONCLUSTERED INDEX INDX_InvoiceType_1
	ON [dbo].[Rista_SaleInvoices] ([InvoiceType])
	INCLUDE ([branchCode],[InvoiceDay],[NetDiscountAmount],[NetDirectChargeAmount],[NetAmount],[NetChargeAmount])
END
GO

IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='INDX_InvoiceId' AND object_id = OBJECT_ID('[dbo].[Rista_SaleItems]'))
BEGIN
	CREATE NONCLUSTERED INDEX [INDX_InvoiceId]
	ON [dbo].[Rista_SaleItems] ([InvoiceID])
	INCLUDE ([AccountName],[NetAmount])
END
go

IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='INDX_InvoiceType' AND object_id = OBJECT_ID('[dbo].[Rista_SaleInvoices]'))
BEGIN
	CREATE NONCLUSTERED INDEX INDX_InvoiceType
	ON [dbo].[Rista_SaleInvoices] ([InvoiceType])
	INCLUDE ([branchCode],[InvoiceDay])
END
Go

IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='INDX_InvoiceType_personcount' AND object_id = OBJECT_ID('[dbo].[Rista_SaleInvoices]'))
BEGIN
	CREATE NONCLUSTERED INDEX [INDX_InvoiceType_personcount]
	ON [dbo].[Rista_SaleInvoices] ([InvoiceType],[Status])
	INCLUDE ([branchCode],[InvoiceDay],[PersonCount])
END
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


CREATE or ALTER   PROCEDURE [dbo].[dbsp_GetDSR_Summary]
(	@Startdt datetime, @Enddt  datetime, @branchCode varchar(20) = null )
AS
BEGIN
	/* exec dbo.dbsp_GetDSR_Summary @Startdt = '2024-09-14',  @Enddt = '2024-09-14', @branchCode='bnmaisha' */
	/** Get Sale Main Category details 
		1.	Here we are taking Netamount of each Item category like Foodsale, Beverage sale...
		2.	total amount is Netamount + charges(service charge /packaging charge..)
		
		Note: There are few sales which are marked as NC (No Charge), we need to exclude those from total sale for food, beverage....
	*******/
	DECLARE @BranchName NVARCHAR(255);
	SELECT @BranchName = A.OutletName FROM MST_OUTLET A WHERE OUTLETCODE = @branchCode;
	
	-- Sales fields
	DECLARE @FoodSaleNet DECIMAL(18, 2), @BeverageSaleNet DECIMAL(18, 2), @LiquorSaleNet DECIMAL(18, 2), @TobaccoSaleNet DECIMAL(18, 2);
	DECLARE @OtherSale1Net DECIMAL(18, 2), @DiscountAmount DECIMAL(18, 2), @ServiceChargeAmount DECIMAL(18, 2), @DirectCharge DECIMAL(18, 2);
	DECLARE @SalesNetTotal DECIMAL(18, 2), @SalesTotalWithSC DECIMAL(18, 2);

	-- Delivery-related fields
	DECLARE @DeliveryFoodSaleNet DECIMAL(18, 2), @DeliveryBeverageSaleNet DECIMAL(18, 2);

	-- Dine-in Sales fields
	DECLARE @DineInFoodSaleNet DECIMAL(18, 2), @DineInBeverageSaleNet DECIMAL(18, 2), @DineInLiquorSaleNet DECIMAL(18, 2), @DineInTobaccoNet DECIMAL(18, 2);
	DECLARE @DineInOthersNet DECIMAL(18, 2), @DineInCovers INT, @ApcDineIn DECIMAL(18, 2); -- (Sale Net - Delivery Sale) / Dine In Covers

	-- Delivery channels (e.g., Zomato, Swiggy)
	DECLARE @ZomatoDeliveryBillsNo INT, @ZomatoDeliverySaleNet DECIMAL(18, 2), @SwiggyDeliveryBillsNo INT, @SwiggyDeliverySaleNet DECIMAL(18, 2);
	DECLARE @DeliveryChannel3BillsNo INT, @DeliveryChannel3SaleNet DECIMAL(18, 2), @DeliveryBillsTotalNo INT, @DeliveryBillsAmountTotal DECIMAL(18, 2);

	-- Aggregator-specific dine-in fields (Zomato, Eazy Diner, etc.)
	DECLARE @ZomatoDineInSaleNet DECIMAL(18, 2), @ZomatoDineInCovers INT, @ZomatoDineInBills INT, @AvgBillAmountZomato DECIMAL(18, 2);
	DECLARE @DineOutDineInSaleNet DECIMAL(18, 2), @DineOutDineInCovers INT, @DineOutDineInBills INT, @AvgBillAmountDineOut DECIMAL(18, 2);
	DECLARE @EazyDinerDineInSaleNet DECIMAL(18, 2), @EazyDinerDineInCovers INT, @EazyDinerDineInBills INT, @AvgBillAmountEazyDiner DECIMAL(18, 2);
	DECLARE @OtherAggregatorDineInSaleNet DECIMAL(18, 2), @OtherAggregatorDineInCovers INT, @OtherAggregatorDineInBills INT, @AvgBillAmountOtherAggregator DECIMAL(18, 2);


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
			@FoodSaleNet =FoodSale, @BeverageSaleNet =BeverageSale, @LiquorSaleNet= LiquorSale, @TobaccoSaleNet=TobaccoSale, 
			@OtherSale1Net =OtherSale, @DiscountAmount = CNS.NetDiscountAmount, 
			@ServiceChargeAmount = CNS.NetChargeAmount, @DirectCharge = CNS.DirectCharge, 
			@SalesNetTotal = CNS.NetSale, @SalesTotalWithSC =(CNS.NetSale + CNS.NetChargeAmount)
	FROM CTE_ITEM_SALE CIS
	LEFT JOIN CTE_NETSALE CNS ON 1 = 1;

	/** Delivery for Food Sale  and Beverage sale 
		For delivery there is no service charge, only packaging charge being added as 25rs as of now 18-09-2024
		So for delivery (all item net amount) + packaging charge
	***/
	SELECT	@DeliveryFoodSaleNet = CASE WHEN SUM(A.DeliveryFoodSale) > 0 THEN SUM(A.DeliveryFoodSale) ELSE 0 END,
			@DeliveryBeverageSaleNet = CASE WHEN SUM(A.DeliveryFoodSale) = 0 AND SUM(A.DeliveryBeverageSale) > 0 THEN SUM(A.DeliveryBeverageSale) ELSE 0 END
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
		@DineInFoodSaleNet =SUM(CASE WHEN AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END),
		@DineInBeverageSaleNet  =SUM(CASE WHEN AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END),
		@DineInLiquorSaleNet = SUM(CASE WHEN AccountName = 'LIQUOR SALE' THEN SIT.NetAmount ELSE 0 END),
		@DineInTobaccoNet = SUM(CASE WHEN AccountName = 'TOBACCO SALE' THEN SIT.NetAmount ELSE 0 END),
		@DineInOthersNet = SUM(CASE WHEN AccountName NOT IN ('Food Sale', 'Beverage Sale', 'TOBACCO SALE', 'LIQUOR SALE') THEN SIT.NetAmount ELSE 0 END)
	FROM Rista_SaleInvoices (NOLOCK) SI 
	INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID
	LEFT JOIN Rista_SaleSourceInfo (NOLOCK) SSI ON SI.InvoiceID = SSI.InvoiceID AND SSI.IsEcomOrder = 1
	WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
		AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
		AND SSI.InvoiceID IS NULL 
		AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled' AND SI.Channel like 'Dine%';
			
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
			AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled' AND SI.Channel like 'Dine%'
	)
	SELECT	
		@DineInCovers = TotalCovers,
		@ApcDineIn = TotalSale / (TotalCovers)
	FROM CTE_APCDINEIN;
	
	/*** DELIVERY DETAILS WITH BILLS COUNT + SALE  -- NOT CONSIDERING TAKEAWAY 
		All delivery order being captured in [Rista_SaleSourceInfo] table where IsECOMorder flag is 1
	*/
	SELECT 	
		@ZomatoDeliveryBillsNo =ISNULL(SUM(CASE WHEN Channel LIKE '%ZOMATO%' THEN 1 ELSE 0 END), 0),
		@ZomatoDeliverySaleNet = ISNULL(SUM(CASE WHEN Channel LIKE '%ZOMATO%' THEN SI.NetAmount ELSE 0 END), 0),
		@SwiggyDeliveryBillsNo =ISNULL(SUM(CASE WHEN Channel LIKE '%SWIGGY%' THEN 1 ELSE 0 END), 0),
		@SwiggyDeliverySaleNet = ISNULL(SUM(CASE WHEN Channel LIKE '%SWIGGY%' THEN SI.NetAmount ELSE 0 END), 0),
		@DeliveryChannel3BillsNo = ISNULL(SUM(CASE WHEN Channel NOT LIKE '%ZOMATO%' AND Channel NOT LIKE '%SWIGGY%' THEN 1 ELSE 0 END), 0),
		@DeliveryChannel3SaleNet = ISNULL(SUM(CASE WHEN Channel NOT LIKE '%ZOMATO%' AND Channel NOT LIKE '%SWIGGY%' THEN SI.NetAmount ELSE 0 END), 0),
		@DeliveryBillsTotalNo = COUNT(DISTINCT SI.InvoiceID),
		@DeliveryBillsAmountTotal = ISNULL(SUM(SI.NETAMOUNT), 0)
	FROM Rista_SaleInvoices (NOLOCK) SI 
	INNER JOIN [dbo].[Rista_SaleSourceInfo] (NOLOCK) SSI ON SI.InvoiceID = SSI.InvoiceID
	WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt 
		AND SSI.IsEcomOrder = 1
		AND SI.branchCode = (CASE WHEN @branchCode IS NULL THEN SI.branchCode ELSE @branchCode END)
			AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled';
				
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
		@ZomatoDineInSaleNet =zomato.DINEINSALE,
		@ZomatoDineInCovers = zomato.DINEINCOVERS,
		@ZomatoDineInBills = zomato.DINEINBILLS,
		@AvgBillAmountZomato= (zomato.DINEINSALE / (case zomato.DINEINBILLS when 0 then 1 else zomato.DINEINBILLS end)),

		-- Dineout Data
		@DineOutDineInSaleNet =dineout.DINEINSALE,
		@DineOutDineInCovers = dineout.DINEINCOVERS,
		@DineOutDineInBills = dineout.DINEINBILLS,
		@AvgBillAmountDineOut = (dineout.DINEINSALE / (case dineout.DINEINBILLS when 0 then 1 else dineout.DINEINBILLS end)),
    
		-- EazyDiner Data
		@EazyDinerDineInSaleNet = eazydiner.DINEINSALE,
		@EazyDinerDineInCovers = eazydiner.DINEINCOVERS,
		@EazyDinerDineInBills = eazydiner.DINEINBILLS,
		@AvgBillAmountEazyDiner = (eazydiner.DINEINSALE / (case eazydiner.DINEINBILLS when 0 then 1 else eazydiner.DINEINBILLS end)),
	
		-- Other Data (ALL - Zomato - Dineout - EasyDiner)
		@OtherAggregatorDineInSaleNet = other.DINEINSALE,
		@OtherAggregatorDineInCovers = other.DINEINCOVERS,
		@OtherAggregatorDineInBills= other.DINEINBILLS,
		@AvgBillAmountOtherAggregator = (other.DINEINSALE / (case other.DINEINBILLS when 0 then 1 else other.DINEINBILLS end))

	FROM  SalesData_OTHER other
	LEFT JOIN SalesData_ZOMATO zomato ON 1 = 1
	LEFT JOIN SalesData_DINEOUT dineout ON 1 = 1
	LEFT JOIN SalesData_EAZYDINER eazydiner ON 1 = 1;

	SELECT 
    @BranchName AS BranchName,
    @branchCode AS BranchCode,

    -- Sales fields
    @FoodSaleNet AS FoodSaleNet,
    @BeverageSaleNet AS BeverageSaleNet,
    @LiquorSaleNet AS LiquorSaleNet,
    @TobaccoSaleNet AS TobaccoSaleNet,
    @OtherSale1Net AS OtherSale1Net,
    @DiscountAmount AS DiscountAmount,
    @ServiceChargeAmount AS ServiceChargeAmount,
    @DirectCharge AS DirectCharge,
    @SalesNetTotal AS SalesNetTotal,
    @SalesTotalWithSC AS SalesTotalWithSC,

    -- Delivery-related fields
    @DeliveryFoodSaleNet AS DeliveryFoodSaleNet,
    @DeliveryBeverageSaleNet AS DeliveryBeverageSaleNet,

    -- Dine-in Sales fields
    @DineInFoodSaleNet AS DineInFoodSaleNet,
    @DineInBeverageSaleNet AS DineInBeverageSaleNet,
    @DineInLiquorSaleNet AS DineInLiquorSaleNet,
    @DineInTobaccoNet AS DineInTobaccoNet,
    @DineInOthersNet AS DineInOthersNet,
    @DineInCovers AS DineInCovers,
    @ApcDineIn AS ApcDineIn,

    -- Delivery channels
    @ZomatoDeliveryBillsNo AS ZomatoDeliveryBillsNo,
    @ZomatoDeliverySaleNet AS ZomatoDeliverySaleNet,
    @SwiggyDeliveryBillsNo AS SwiggyDeliveryBillsNo,
    @SwiggyDeliverySaleNet AS SwiggyDeliverySaleNet,
    @DeliveryChannel3BillsNo AS DeliveryChannel3BillsNo,
    @DeliveryChannel3SaleNet AS DeliveryChannel3SaleNet,
    @DeliveryBillsTotalNo AS DeliveryBillsTotalNo,
    @DeliveryBillsAmountTotal AS DeliveryBillsAmountTotal,

    -- Aggregator-specific dine-in fields
    @ZomatoDineInSaleNet AS ZomatoDineInSaleNet,
    @ZomatoDineInCovers AS ZomatoDineInCovers,
    @ZomatoDineInBills AS ZomatoDineInBills,
    @AvgBillAmountZomato AS AvgBillAmountZomato,

    @DineOutDineInSaleNet AS DineOutDineInSaleNet,
    @DineOutDineInCovers AS DineOutDineInCovers,
    @DineOutDineInBills AS DineOutDineInBills,
    @AvgBillAmountDineOut AS AvgBillAmountDineOut,

    @EazyDinerDineInSaleNet AS EazyDinerDineInSaleNet,
    @EazyDinerDineInCovers AS EazyDinerDineInCovers,
    @EazyDinerDineInBills AS EazyDinerDineInBills,
    @AvgBillAmountEazyDiner AS AvgBillAmountEazyDiner,

    @OtherAggregatorDineInSaleNet AS OtherAggregatorDineInSaleNet,
    @OtherAggregatorDineInCovers AS OtherAggregatorDineInCovers,
    @OtherAggregatorDineInBills AS OtherAggregatorDineInBills,
    @AvgBillAmountOtherAggregator AS AvgBillAmountOtherAggregator;


END
GO