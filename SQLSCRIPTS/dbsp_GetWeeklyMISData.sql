USE [BELLONA_LIVE]
GO
/****** Object:  StoredProcedure [dbo].[dbsp_GetWeeklyMISData]    Script Date: 29-10-2024 00:06:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER     PROCEDURE [dbo].[dbsp_GetWeeklyMISData]
(	@WEEK VARCHAR(20), @FINANCIALYEAR VARCHAR(10), @branchCode varchar(20) = null, @clusterId int = 0, @cityId int = 0,
	@UserId uniqueIdentifier = null, @MenuId   Int= 0 )
AS
BEGIN
	/*
		EXEC [dbo].[dbsp_GetWeeklyMISData] @WEEK = 'WEEK 30', @FINANCIALYEAR ='2024-25'
	*/
	DECLARE @OUTLETS TABLE(OutletID INT, BRANCHCODE VARCHAR(20), BRANCHNAME VARCHAR(255), CLUSTERNAME VARCHAR(255), CityName VARCHAR(100))
	DECLARE @Startdt datetime, @Enddt  datetime
	DECLARE @ACTUALSALE DECIMAL(15,2), @NETSALE DECIMAL(15,2), @BUDGET DECIMAL(15,2), @VARIANCE DECIMAL(15,2), @COVER INT, @APC DECIMAL(8,2)
	DECLARE @DINEINSALE DECIMAL(15,2), @DELIVERYSALE DECIMAL(15,2), @TAKEAWAY DECIMAL(15,2), @SALEPERSQFT DECIMAL(15,2)
	DECLARE @NetDiscountAmount DECIMAL(15,2), @NetChargeAmount DECIMAL(15,2), @DirectCharge DECIMAL(15,2), @OUTLETAREA DECIMAL(20,2)
	DECLARE @GROSSEXPENSE DECIMAL(15,2), @GROSSPROFIT DECIMAL(15,2), @DAYEXPENSE DECIMAL(15,2), @NETEXPENSE DECIMAL(15,2)
	DECLARE @NETPROFIT DECIMAL(15,2), @OTHERSALE DECIMAL(20,2)

	IF @branchCode = '0' OR @branchCode = 'null' or @branchCode = '' OR LTRIM(RTRIM(@branchCode)) = ''  SET @branchCode = NULL

	SELECT @Startdt = MIN([DATE]), @Enddt = MAX([DATE]) FROM Transaction_DateRange WHERE [WeekNo] = @WEEK and [FinancialYear] = @FINANCIALYEAR;

	INSERT @OUTLETS (OutletID, BRANCHCODE, BRANCHNAME, CLUSTERNAME, CityName)
	SELECT distinct MO.OutletID, OutletCode BRANCHCODE, OutletName BRANCHNAME, MC.ClusterName, MCT.CityName
	FROM MST_OUTLET MO 
	INNER JOIN MST_Clusters MC ON MO.ClusterID = MC.ClusterID
	INNER JOIN MST_Cities MCT ON MC.CityID = MCT.CityID
	INNER JOIN [dbo].[Link_Menu_Outlet] LMO on LMO.OutletId = Mo.OutletId and Menuid = (CASE WHEN @MenuId = 0 THEN MenuId ELSE @MenuId END)          
	INNER JOIN [dbo].[Um_Mst_UserAccess] UMU on UMU.AccessId = LMO.AccessId and Userid =  (CASE WHEN @UserId IS NULL THEN UserId ELSE @UserId END)
	WHERE	MC.CityID =  (CASE WHEN ISNULL(@cityId, 0) = 0 THEN MC.CityID ELSE @cityId END)
			AND MO.ClusterID = (CASE WHEN ISNULL(@clusterId, 0) = 0 THEN MO.ClusterID ELSE @clusterId END)
			AND OutletCode = (CASE WHEN ISNULL(@branchCode , '0') = '0' THEN OutletCode ELSE @branchCode END)  
	
	SELECT	@DINEINSALE = SUM([DINE-IN]), 
			@TAKEAWAY= SUM(TAKEAWAY), 
			@COVER= SUM(PersonCount),
			@NETSALE = SUM(NETAMOUNT),
			@NetDiscountAmount = SUM(NetDiscountAmount) ,
			@NetChargeAmount = SUM(NetChargeAmount),
			@DirectCharge =  SUM(NetDirectChargeAmount) 
	FROM (
		SELECT	CASE WHEN SI.CHANNEL LIKE 'DINE%' THEN (SI.NETAMOUNT) ELSE 0	 END [DINE-IN],
				CASE WHEN SI.CHANNEL LIKE 'TAKEAWAY%' THEN (SI.NETAMOUNT) ELSE 0 END [TAKEAWAY],
				SI.PersonCount, SI.NetAmount, SI.ChargeAmount NetChargeAmount, SI.NetDiscountAmount, SI.NetDirectChargeAmount
		FROM Rista_SaleInvoices (NOLOCK) SI
		INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
		WHERE	SI.InvoiceDay BETWEEN @Startdt and @Enddt 
				AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled'
		) A
	

	/*	
	SELECT	@ACTUALSALE = SUM(SI.NETAMOUNT),
			@NetDiscountAmount = SUM(SI.NetDiscountAmount) ,
			@NetChargeAmount = SUM(SI.NetChargeAmount) ,
			@DirectCharge =  SUM(SI.NetDirectChargeAmount) 
	FROM Rista_SaleInvoices (NOLOCK) SI 
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
	WHERE	SI.InvoiceDay BETWEEN @Startdt and @Enddt
			AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'

	
	SELECT	@COVER =  SUM(SI.PERSONCOUNT), 
			@DINEINSALE = SUM(SI.NETAMOUNT)

	FROM Rista_SaleInvoices (NOLOCK) SI
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
	WHERE	SI.InvoiceDay BETWEEN @Startdt and @Enddt 
			AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled' AND SI.Channel like 'Dine%'
	
	SELECT	@TAKEAWAY = SUM(SI.NETAMOUNT)
	FROM Rista_SaleInvoices (NOLOCK) SI
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
	WHERE	SI.InvoiceDay BETWEEN @Startdt and @Enddt 
			AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled' AND SI.Channel like 'Takea%'
	*/

	/** Takea away food are not part of delivery and Dine -in */
	SELECT	@DELIVERYSALE =  SUM(SI.NetAmount)
	FROM Rista_SaleInvoices (NOLOCK) SI
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
	INNER JOIN Rista_SaleSourceInfo (NOLOCK) SSI  ON SI.InvoiceID = SSI.InvoiceID AND SSI.IsEcomOrder = 1
	WHERE	SI.InvoiceDay BETWEEN @Startdt and @Enddt 
			AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled'

	SELECT	@OTHERSALE =  SUM(SI.NetAmount)
	FROM Rista_SaleInvoices (NOLOCK) SI
	INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE
	INNER JOIN Rista_SaleSourceInfo (NOLOCK) SSI  ON SI.InvoiceID = SSI.InvoiceID AND SSI.IsEcomOrder = 0
	WHERE	SI.InvoiceDay BETWEEN @Startdt and @Enddt 
			AND SI.InvoiceType <> 'NC'  AND SI.Status <> 'Cancelled'
			AND Channel NOT LIKE 'DINE%' AND Channel NOT LIKE 'TAKEAWAY%'

	SELECT @BUDGET = SUM(SBD.CategoryAmount) 
	FROM Transaction_SalesBudgetDetails (NOLOCK) SBD
	INNER JOIN Transaction_SalesBudget (NOLOCK) SB ON SB.SalesBudgetID = SBD.SalesBudgetID
	INNER JOIN @OUTLETS MO ON SB.OutletID = MO.OutletID
	WHERE SBD.[Date] BETWEEN @Startdt AND @Enddt;

	SELECT @GROSSEXPENSE = TDE.D_ProductionCost_Total
	FROM Transaction_Daily_Expense  TDE
	INNER JOIN  Transaction_DateRange TDR ON TDE.WeekNo = TDR.Week_number AND TDE.ExpenseYear = TDR.[Year] AND TDE.ExpenseMonth = TDR.[Month] 
	INNER JOIN @OUTLETS MO ON TDE.OutletId = MO.OutletID
	WHERE TDR.[WeekNo] = @WEEK and TDR.[FinancialYear] = @FINANCIALYEAR

	/** All other expenses */
	SELECT TOP 1 @DAYEXPENSE = ((TME.Utilities_Total + TME.Equipement_Total + TME.FinanceCharge_Total + TME.ITSoftware_Total 
							+ TME.LabourCost_Total + TME.Maintenance_Total + TME.MarketingCost_Total + TME.OtherExpense_Total
							+ TME.Property_Total + TME.Royalty_Total) / 30)
	FROM Transaction_Monthly_Expense  TME
	INNER JOIN  Transaction_DateRange TDR ON TME.ExpenseYear = TDR.[Year] AND TME.ExpenseMonth = TDR.[Month] 
	INNER JOIN @OUTLETS MO ON TME.OutletId = MO.OutletID
	WHERE TDR.[WeekNo] = @WEEK and TDR.[FinancialYear] = @FINANCIALYEAR
	
	SET @NETEXPENSE = @GROSSEXPENSE + (ISNULL(@DAYEXPENSE, 0) * 7)

	SELECT @OUTLETAREA = SUM(OUTLETAREA) 
	FROM MST_OUTLET (NOLOCK) MST_O
	INNER JOIN @OUTLETS MO ON MST_O.OutletID = MO.OutletID;

	SET @ACTUALSALE = ISNULL(@NETSALE, 0.00) +  ISNULL(@NetChargeAmount, 0.00)
	SET @OTHERSALE = ISNULL(@OTHERSALE, 0.00) + ISNULL(@DirectCharge, 0.00)

	SELECT	ISNULL(@NETSALE, 0.00) NETSALE, 
			/*ISNULL(@GROSSEXPENSE, 0.00) GROSSEXPENSE, ISNULL(@NETEXPENSE, 0.00) NETEXPENSE, @DAYEXPENSE DAYEXPENSE, */
			ISNULL(@ACTUALSALE, 0.00) ACTUALSALE, ISNULL(@BUDGET, 0.00) BUDGET, 
			ISNULL(@ACTUALSALE - @BUDGET, 0.00) as VARIANCE, 
			ISNULL(@NETSALE, 0.00) - ISNULL(@GROSSEXPENSE, 0.00) AS GROSSPROFIT, 
			ISNULL(@ACTUALSALE, 0.00) - ISNULL(@NETEXPENSE, 0.00) AS NETPROFIT, 
			ISNULL((@ACTUALSALE / NULLIF(@OUTLETAREA, 0)), 0.00) SALEPERSQFT,
			ISNULL(@ACTUALSALE / NULLIF(@COVER, 0), 0.00) APC, 
			isnull(@DINEINSALE, 0) DINEINSALE, ISNULL(@DELIVERYSALE, 0.00) DELIVERYSALE, isnull(@TAKEAWAY, 0) TakeAway, ISNULL(@OTHERSALE, 0.00) OtherSale, 
			ISNULL(@NetDiscountAmount, 0.00) NetDiscountAmount, ISNULL(@NetChargeAmount, 0.00) NetChargeAmount, 
			ISNULL(@DirectCharge, 0.00) DirectCharge, ISNULL(@COVER, 0.00) COVERS, (ISNULL(@ACTUALSALE, 0.00) / 7) ADC,

			(ISNULL(@ACTUALSALE, 0.00) / NULLIF(ISNULL(@BUDGET, 0.00), 0)) * 100 SALEPERC,
			((ISNULL(@ACTUALSALE, 0.00) - ISNULL(@BUDGET, 0.00)) / NULLIF(ISNULL(@BUDGET, 0.00), 0)) * 100 SALEVARPERC,
			((ISNULL(@NETSALE, 0.00) - ISNULL(@GROSSEXPENSE, 0.00)) / NULLIF(@NETSALE, 0)) * 100 GROSSPERC,
			((ISNULL(@ACTUALSALE, 0.00) - ISNULL(@NETEXPENSE, 0.00)) / NULLIF(@ACTUALSALE, 0)) * 100 NETPERC,
			(ISNULL(@DINEINSALE, 0.00) / NULLIF(@NETSALE, 0)) * 100 DININPERC,			
			(ISNULL(@DELIVERYSALE, 0.00) / NULLIF(@NETSALE, 0)) * 100 DELIVERYPERC
			/*(ISNULL(@TAKEAWAY, 0.00) / NULLIF(@NETSALE, 0)) * 100 TAKEAWAYPERC,
			(ISNULL(@OTHERSALE, 0.00) / NULLIF(@NETSALE, 0)) * 100 OTHERPERC */


END
