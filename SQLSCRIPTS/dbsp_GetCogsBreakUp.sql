USE [BELLONA_LIVE]
GO
/****** Object:  StoredProcedure [dbo].[dbsp_GetCogsBreakUp]    Script Date: 29-10-2024 00:24:57 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



ALTER   PROCEDURE [dbo].[dbsp_GetCogsBreakUp]
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
        EXEC [dbo]. [dbsp_GetCogsBreakUp] @WEEK = 'WEEK 29', @FINANCIALYEAR = '2024-25'
    */

DECLARE @Startdt DATETIMEOFFSET,
            @Enddt DATETIMEOFFSET;

	IF @branchCode = '0' OR @branchCode = 'null' or @branchCode = '' OR LTRIM(RTRIM(@branchCode)) = ''  SET @branchCode = NULL

SELECT @Enddt = ISNULL(MAX([DATE]), GETDATE()) -- Default to current date if no date found
    FROM Transaction_DateRange 
    WHERE WeekNo = @WEEK 
      AND FinancialYear = @FINANCIALYEAR;

SELECT @Startdt = DATEADD(WEEK, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, @Enddt), 0));

DECLARE @OUTLETS TABLE (
        BRANCH_ID INT PRIMARY KEY,
        BRANCHCODE VARCHAR(20), 
        BRANCHNAME VARCHAR(255), 
        CLUSTERNAME VARCHAR(255), 
        CITYNAME VARCHAR(100)
    );

INSERT INTO @OUTLETS (BRANCH_ID, BRANCHCODE, BRANCHNAME, CLUSTERNAME, CITYNAME)
    SELECT DISTINCT 
        MO.OutletID, 
        MO.OutletCode, 
        MO.OutletName, 
        MC.ClusterName, 
        MCT.CityName
    FROM MST_OUTLET MO WITH (NOLOCK)
    INNER JOIN MST_Clusters MC ON MO. ClusterID = MC.ClusterID
    INNER JOIN MST_Cities MCT ON MC.CityID = MCT. CityID
	INNER JOIN [dbo].[Link_Menu_Outlet] LMO on LMO.OutletId = Mo.OutletId and Menuid = (CASE WHEN @MenuId = 0 THEN MenuId ELSE @MenuId END)          
	INNER JOIN [dbo].[Um_Mst_UserAccess] UMU on UMU.AccessId = LMO.AccessId and Userid =  (CASE WHEN @UserId IS NULL THEN UserId ELSE @UserId END)
    WHERE (ISNULL(@cityId, 0) = 0 OR MC.CityID = @cityId)
      AND (ISNULL(@clusterId, 0) = 0 OR MO. ClusterID = @clusterId)
      AND (@branchCode IS NULL OR MO. OutletCode = @branchCode);

WITH CTE_Sales AS (
        SELECT 
            CASE 
                WHEN I.AccountName IN ('Beverage Sale', 'Food Sale', 'Liquor Sale') 
                THEN I.AccountName
                ELSE 'Other Sale'
            END AS AccountName,
            SUM(ISNULL(I.NetAmount, 0.00)) AS NetAmount
        FROM Rista_SaleItems I WITH (NOLOCK)
        INNER JOIN Rista_SaleInvoices SI WITH (NOLOCK) ON I.InvoiceID = SI. InvoiceID
        INNER JOIN @OUTLETS MO ON SI.branchCode = MO. BRANCHCODE
        INNER JOIN Transaction_DateRange TDR WITH (NOLOCK) ON SI. InvoiceDay = TDR. [Date]
        WHERE SI. InvoiceDay BETWEEN @Startdt AND @Enddt
          AND SI. InvoiceType <> 'NC' -- Exclude non-cash invoices
          AND SI. Status <> 'Cancelled' -- Exclude cancelled invoices
        GROUP BY 
            CASE 
                WHEN I.AccountName IN ('Beverage Sale', 'Food Sale', 'Liquor Sale') 
                THEN I.AccountName
                ELSE 'Other Sale'
            END
    ),
    CTE_Cost AS (
        SELECT 
            ExpenseType,
            SUM(Cost) AS Cost
        FROM (
            SELECT 
                DE. OutletId,
                CAST(ISNULL(D_ProductionCost_Food, 0) AS DECIMAL(18, 2)) AS FoodCost,
                CAST(ISNULL(D_ProductionCost_Beverage, 0) AS DECIMAL(18, 2)) AS BeverageCost,
                CAST(ISNULL(D_ProductionCost_Liquor, 0) AS DECIMAL(18, 2)) AS LiquorCost,
                CAST(ISNULL(D_ProductionCost_Other, 0) + ISNULL(D_ProductionCost_Tobacco, 0) AS DECIMAL(18, 2)) AS OtherCost
            FROM Transaction_Daily_Expense DE WITH (NOLOCK)
            INNER JOIN Transaction_DateRange DR ON DR. Year = DE. ExpenseYear AND DE. ExpenseMonth = DR. Month AND DE. ExpenseDay = DR. DayNo 
            INNER JOIN @OUTLETS O ON DE. OutletId = O.BRANCH_ID
            WHERE DR. WeekNo = @WEEK AND DR. FinancialYear = @FINANCIALYEAR
        ) AS SourceTable
        UNPIVOT (
            Cost FOR ExpenseType IN (FoodCost, BeverageCost, LiquorCost, OtherCost)
        ) AS UnpivotedTable
        GROUP BY ExpenseType
    ),
    TotalSales AS (
        SELECT ISNULL(SUM(NetAmount), 0.00) AS TotalSale FROM CTE_Sales
    ),
    TotalExpense AS (
        SELECT ISNULL(SUM(Cost), 0.00) AS TotalCost FROM CTE_Cost
    )
    SELECT 
        CC.ExpenseType AS Category, 
        ISNULL(CC.Cost,0.0) AS Amount,
        ISNULL(CAST(CC.Cost / NULLIF(CS. NetAmount, 0) AS DECIMAL(16, 2)), 0.0) AS Percentage
    FROM CTE_Cost CC
    LEFT JOIN CTE_Sales CS ON (CC.ExpenseType LIKE '%' + REPLACE(CS. AccountName, ' Sale', '') + '%')
    UNION ALL  
    SELECT 
        'TotalCost' AS Category,
        TE. TotalCost AS Amount,
        ISNULL(CAST(TE. TotalCost / NULLIF(TS. TotalSale, 0) AS DECIMAL(16, 2)), 0.0) AS Percentage
    FROM TotalExpense TE
    CROSS JOIN TotalSales TS;
END
