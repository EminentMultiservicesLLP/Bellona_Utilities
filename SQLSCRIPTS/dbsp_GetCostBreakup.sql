USE [BELLONA_LIVE]
GO
/****** Object:  StoredProcedure [dbo].[dbsp_GetCostBreakup]    Script Date: 29-10-2024 00:26:21 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


--Meghna  --->


ALTER   PROCEDURE [dbo].[dbsp_GetCostBreakup] 
    @WEEK VARCHAR(20), 
    @FINANCIALYEAR VARCHAR(10), 
    @branchCode VARCHAR(20) = NULL, 
    @clusterId INT = 0, 
    @cityId INT = 0,
	@UserId uniqueIdentifier = null, @MenuId   Int= 0
AS
BEGIN

	/*
        Example Execution:
        EXEC [dbo]. [dbsp_GetCostBreakup] @WEEK = 'WEEK 19', @FINANCIALYEAR = '2024-25'
    */
	
	DECLARE @Startdt DATETIMEOFFSET, @Enddt DATETIMEOFFSET;

	IF @branchCode = '0' OR @branchCode = 'null' or @branchCode = '' OR LTRIM(RTRIM(@branchCode)) = ''  SET @branchCode = NULL

SELECT @Enddt = MAX([DATE]) 
    FROM Transaction_DateRange 
    WHERE WeekNo = @WEEK 
      AND FinancialYear = @FINANCIALYEAR;

SET @Startdt = DATEADD(WEEK, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, @Enddt), 0));

DECLARE @OUTLETS TABLE (
        BRANCH_ID INT,
        BRANCHCODE VARCHAR(20), 
        BRANCHNAME VARCHAR(255), 
        CLUSTERNAME VARCHAR(255), 
        CITYNAME VARCHAR(100),
        RENT_TYPE INT
    );

INSERT INTO @OUTLETS (BRANCH_ID, BRANCHCODE, BRANCHNAME, CLUSTERNAME, CITYNAME, RENT_TYPE)
    SELECT DISTINCT 
        MO.OutletID, 
        MO.OutletCode, 
        MO.OutletName, 
        MC.ClusterName, 
        MCT.CityName,
        MO. RentType
    FROM MST_OUTLET MO WITH (NOLOCK)
    INNER JOIN MST_Clusters MC ON MO. ClusterID = MC.ClusterID
    INNER JOIN MST_Cities MCT ON MC.CityID = MCT. CityID
	INNER JOIN [dbo].[Link_Menu_Outlet] LMO on LMO.OutletId = Mo.OutletId and Menuid = (CASE WHEN @MenuId = 0 THEN MenuId ELSE @MenuId END)          
	INNER JOIN [dbo].[Um_Mst_UserAccess] UMU on UMU.AccessId = LMO.AccessId and Userid =  (CASE WHEN @UserId IS NULL THEN UserId ELSE @UserId END)
    WHERE (@cityId = 0 OR MC.CityID = @cityId)
      AND (@clusterId = 0 OR MO. ClusterID = @clusterId)
      AND (@branchCode IS NULL OR MO. OutletCode = @branchCode);

;WITH CTE_SalesData AS (
        SELECT 
            O.BRANCH_ID, 
            O.RENT_TYPE, 
            SUM(ISNULL(SI. NetAmount, 0.00)) AS TotalSale
        FROM Rista_SaleInvoices SI WITH (NOLOCK)
        INNER JOIN @OUTLETS O ON SI.branchCode = O.BRANCHCODE
        INNER JOIN Transaction_DateRange TDR WITH (NOLOCK) ON SI. InvoiceDay = TDR. [Date]
        WHERE SI. InvoiceDay BETWEEN @Startdt AND @Enddt
          AND SI. InvoiceType <> 'NC' -- Exclude non-cash invoices
          AND SI. Status <> 'Cancelled' -- Exclude cancelled invoices
        GROUP BY O.BRANCH_ID, O.RENT_TYPE
    ),

CTE_CreditCardTotalSale AS (
        SELECT 
            O.BRANCH_ID,          
            SUM(ISNULL(Pay.Amount, 0.00)) AS TotalSale
        FROM [Rista_SalePayments] Pay WITH (NOLOCK)
		inner join Rista_SaleInvoices SI on SI. InvoiceID = Pay.InvoiceID 
        INNER JOIN @OUTLETS O ON SI.branchCode = O.BRANCHCODE
        INNER JOIN Transaction_DateRange TDR WITH (NOLOCK) ON SI. InvoiceDay = TDR. [Date]
        WHERE SI. InvoiceDay BETWEEN @Startdt AND @Enddt
			AND Mode ='Credit Card Sales'
          AND SI. InvoiceType <> 'NC' -- Exclude non-cash invoices
          AND SI. Status <> 'Cancelled' -- Exclude cancelled invoices
        GROUP BY O.BRANCH_ID
    ),

CTE_WeeklyExpenses AS (
        SELECT 
            WE. OutletId AS BRANCH_ID,
			ISNULL(WE. Total_EquipmentHireCharges,0) as EquipmentHireCharges,
			ISNULL(WE. Total_BusinessPromotionMarketing,0) as BusinessPromotionMarketing,			
			ISNULL(WE. Total_NonFoodConsumable,0) as NonFoodConsumable,
			ISNULL(WE. Total_LegalFees,0) as LegalFees,
			ISNULL(WE. Total_LicenseFees,0) as LicenseFees,
			ISNULL(WE. Total_ManPowerCost,0) as ManPowerCost,
			ISNULL(WE. Total_OtherOperational,0) as OtherOperational,
			ISNULL(WE. Total_RepairMaintenance,0) as RepairMaintenance,
			ISNULL(WE. Total_TelephoneInternet,0) as TelephoneInternet,
			ISNULL(WE. Total_UtilityEnergyCost,0) as UtilityEnergyCost, 
			ISNULL(WE. RentOccupationCost_CAMCharges, 0) AS CAMCharges,
            ISNULL(WE. RentOccupationCost_PropertyTax, 0) AS PropertyTax,
            ISNULL(WE. RentOccupationCost_PropertyTaxMall, 0) AS PropertyTaxMall,
            ISNULL(WE. RentOccupationCost_RentRevenueCharges, 0) AS RentRevenueCharges,
            ISNULL(WE. RentOccupationCost_RentRevenuePerc, 0) AS RentRevenuePerc,
			ISNULL(WE. FinanceCost_BankCharges,0) as FinanceCost_BankCharges,
			ISNULL(WE. FinanceCost_CommissionOnAggregators,0) as CommissionOnAggregators,
			ISNULL(WE. FinanceCost_CommissionOnCardSettlement,0) as CommissionOnCardSettlement
        FROM Transaction_WeeklyExpense WE
		WHERE WE. FinancialYear = @FINANCIALYEAR AND WE. Weeks = @WEEK
    ),	

CTE_RentCosts AS (
        SELECT 
            WE. BRANCH_ID,
            ISNULL(WE. CAMCharges, 0) AS CAMCharges,
            ISNULL(WE. PropertyTax, 0) AS PropertyTax,
            ISNULL(WE. PropertyTaxMall, 0) AS PropertyTaxMall, 
			(ISNULL(WE. RentRevenueCharges, 0))+ (
			CASE 
				WHEN S.RENT_TYPE = 1 THEN (ISNULL(WE. RentRevenuePerc, 0) * S.TotalSale) / 100
				WHEN S.RENT_TYPE = 3 THEN 
					CASE 
						WHEN ISNULL(WE. RentRevenuePerc, 0) = 0 AND ISNULL(WE. RentRevenueCharges, 0) = 0 THEN 0
						WHEN ISNULL(WE. RentRevenueCharges, 0) = 0 AND ISNULL(WE. RentRevenuePerc, 0) > 0 THEN (ISNULL(WE. RentRevenuePerc, 0) * S.TotalSale) / 100
						WHEN (ISNULL(WE. RentRevenuePerc, 0) * S.TotalSale) / 100 < WE. RentRevenueCharges THEN 0
						ELSE ((ISNULL(WE. RentRevenuePerc, 0) * S.TotalSale) / 100) - WE. RentRevenueCharges
					END
				ELSE 0
			END ) AS RentOccupationCost
        FROM CTE_SalesData S
        INNER JOIN CTE_WeeklyExpenses WE ON S.BRANCH_ID = WE. BRANCH_ID
    ),

CTE_Total_OccupationRent AS (
		SELECT 
			CAST(SUM(CAMCharges) AS DECIMAL(16, 2)) +
			CAST(SUM(PropertyTax) AS DECIMAL(16, 2)) +
			CAST(SUM(PropertyTaxMall) AS DECIMAL(16, 2)) +
			CAST(SUM(RentOccupationCost) AS DECIMAL(16, 2)) AS TotalRent
		FROM CTE_RentCosts		
			
),	
	CTE_AggregateCommission_1 As (
	SELECT
		 SUM(ISNULL(rsp. Amount,0)) as Commision_1	
		FROM Rista_SaleInvoices (NOLOCK) rsi
		INNER JOIN @OUTLETS MO ON RSI.branchCode = MO. BRANCHCODE
		INNER JOIN Rista_SalePayments (NOLOCK) rsp ON rsi. InvoiceID = rsp. InvoiceID
		LEFT JOIN Rista_SaleSourceInfo (NOLOCK) rssi ON rsi. InvoiceID = rssi. InvoiceID
		WHERE rsi. InvoiceDay BETWEEN @Startdt and @Enddt and rssi. InvoiceID IS NULL
				AND RSI. InvoiceType <> 'NC' AND RSI. Status <> 'Cancelled'
				AND (rsp.mode like '%EASY%DINER%'   
				OR rsp.mode like '%Zomato%PRO%' 
				OR rsp.mode like '%Dine%Out%')  	
		),
	CTE_AggregateCommission_2 As(
	SELECT 			
		(ISNULL(SUM(CASE WHEN Channel LIKE '%ZOMATO%' THEN SI. NetAmount ELSE 0 END), 0)) +
		(ISNULL(SUM(CASE WHEN Channel LIKE '%SWIGGY%' THEN SI. NetAmount ELSE 0 END), 0)) as Commission_2
			FROM Rista_SaleInvoices (NOLOCK) SI 
			INNER JOIN @OUTLETS MO ON SI.branchCode = MO. BRANCHCODE
			INNER JOIN [dbo]. [Rista_SaleSourceInfo] (NOLOCK) SSI ON SI. InvoiceID = SSI. InvoiceID
			WHERE SI. InvoiceDay BETWEEN @Startdt and @Enddt 
				AND SSI. IsEcomOrder = 1				
				AND SI. InvoiceType <> 'NC' AND SI. Status <> 'Cancelled'
				
),
	CTE_FinanceCost AS(
		SELECT 
		Cast(ISNULL(WE. FinanceCost_BankCharges, 0) +		
		(Case 
		WHEN ISNULL (WE. CommissionOnCardSettlement, 0) = 0 Then 0
		ELSE ((ISNULL(WE. CommissionOnCardSettlement, 0) * S.TotalSale) / 100)
		END)+ (Case 
		WHEN ISNULL (WE. CommissionOnAggregators, 0) = 0 Then 0
		ELSE ((ISNULL(WE. CommissionOnAggregators, 0) * Total_commision) / 100)
		END )AS decimal(16,2)) As TotalFinanceCost			
		FROM CTE_WeeklyExpenses WE
		INNER JOIN CTE_CreditCardTotalSale S ON S.BRANCH_ID = WE. BRANCH_ID			
		Join 
		(Select Commision_1+ Commission_2 as Total_commision from CTE_AggregateCommission_1 join CTE_AggregateCommission_2 on 1=1
		) As Commission_total on 1=1
	),

CTE_Total_CostBreakUp AS (
		SELECT 
		CAST(SUM(EquipmentHireCharges) AS DECIMAL(16, 2)) AS EquipmentHireCharges,
		CAST(SUM(BusinessPromotionMarketing) AS DECIMAL(16, 2)) AS BusinessPromotionMarketing,
		CAST(SUM(NonFoodConsumable) AS DECIMAL(16, 2)) AS NonFoodConsumable,
		CAST(SUM(LegalFees) AS DECIMAL(16, 2)) AS LegalFees,
		CAST(SUM(LicenseFees) AS DECIMAL(16, 2)) AS LicenseFees,
		CAST(SUM(ManPowerCost) AS DECIMAL(16, 2)) AS ManPowerCost,
		CAST(SUM(OtherOperational) AS DECIMAL(16, 2)) AS OtherOperational,
		CAST(SUM(RepairMaintenance) AS DECIMAL(16, 2)) AS RepairMaintenance,		
		CAST(SUM(TelephoneInternet) AS DECIMAL(16, 2)) AS TelephoneInternet,
		CAST(SUM(UtilityEnergyCost) AS DECIMAL(16, 2)) AS UtilityEnergyCost,
		CAST(TotalFinanceCost AS DECIMAL(16, 2)) as TotalFinanceCost,
		CAST(R.TotalRent as DECIMAL(16,2)) as TotalRent	
		FROM CTE_SalesData S
        INNER JOIN CTE_WeeklyExpenses WE ON S.BRANCH_ID = WE. BRANCH_ID
		Join CTE_FinanceCost F on 1=1
		Join CTE_Total_OccupationRent R on 1=1
		Group by F.TotalFinanceCost, R.TotalRent
	)
	   	 SELECT 
			CostBreakupCategory AS CostBreakUp,
			CAST(Amount AS DECIMAL(16, 2)) AS Amount,
			CAST((Amount * 100.0 / TotalAmount) AS DECIMAL(16, 2)) AS Percentage
		FROM (
			SELECT 
				EquipmentHireCharges,
				BusinessPromotionMarketing,
				NonFoodConsumable,
				LegalFees,
				LicenseFees,
				ManPowerCost,
			OtherOperational,
				RepairMaintenance,		
				TelephoneInternet,
				UtilityEnergyCost,
			TotalFinanceCost,
			TotalRent	
			FROM CTE_Total_CostBreakUp 
		) AS Summary
		JOIN (
			SELECT 
				(EquipmentHireCharges + BusinessPromotionMarketing + NonFoodConsumable + LegalFees+ LicenseFees +ManPowerCost+OtherOperational + RepairMaintenance
				+ TelephoneInternet+ UtilityEnergyCost + TotalFinanceCost +TotalRent) AS TotalAmount
			FROM CTE_Total_CostBreakUp
		) AS TotalSummary ON 1 = 1
		UNPIVOT (
			Amount FOR CostBreakupCategory IN (EquipmentHireCharges , BusinessPromotionMarketing , NonFoodConsumable, LegalFees, LicenseFees,ManPowerCost,OtherOperational , RepairMaintenance
				, TelephoneInternet, UtilityEnergyCost ,TotalFinanceCost, TotalRent )
		) AS Unpvt
		WHERE Amount > 0
END
go