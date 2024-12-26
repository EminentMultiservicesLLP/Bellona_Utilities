USE [BELLONA_LIVE]
GO
/****** Object:  StoredProcedure [dbo].[dbsp_GetOtherSaleVsBudgetTrend]   Script Date: 15-10-2024 14:42:34 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER  PROCEDURE [dbo].[dbsp_GetOtherSaleVsBudgetTrend]
(@WEEK VARCHAR(20), @FINANCIALYEAR VARCHAR(10), @branchCode varchar(20) = null, @clusterId int = 0, @cityId int = 0,
@UserId uniqueIdentifier = null, @MenuId   Int= 0 )
AS
BEGIN
	/*
		EXEC [dbo].[dbsp_GetOtherSaleVsBudgetTrend] @WEEK = 'WEEK 13', @FINANCIALYEAR = '2024-25'
	
	*/

	--DECLARE @WEEK VARCHAR(20) = 'WEEK 27', @FINANCIALYEAR VARCHAR(10) = '2024-25',  @branchCode varchar(20) = 'bnahisha', @clusterId int = 0, @cityId int = 0 
	DECLARE @Startdt DATETIMEOFFSET, @Enddt  DATETIMEOFFSET
	DECLARE @OUTLETS TABLE(OutletID INT, BRANCHCODE VARCHAR(20), BRANCHNAME VARCHAR(255), CLUSTERNAME VARCHAR(255), CityName VARCHAR(100))
	DECLARE @cols AS NVARCHAR(500), @colsNullhandling NVARCHAR(1000), @query AS NVARCHAR(MAX);

	IF @branchCode = '0' OR @branchCode = 'null' or @branchCode = '' OR LTRIM(RTRIM(@branchCode)) = ''  SET @branchCode = NULL

	SELECT @Enddt = MAX([DATE]) FROM Transaction_DateRange WHERE WeekNo = @WEEK AND FinancialYear = @FINANCIALYEAR;
	--Start date from 12 week prior
	SELECT @Startdt = DATEADD(WEEK, -12, DATEADD(WEEK, DATEDIFF(WEEK, 0, @Enddt), 0));

	INSERT @OUTLETS (OutletID, BRANCHCODE, BRANCHNAME, CLUSTERNAME, CityName)
	SELECT distinct MO.OutletID, OutletCode BRANCHCODE, OutletName BRANCHNAME, MC.ClusterName, MCT.CityName
	FROM MST_OUTLET MO 
	INNER JOIN MST_Clusters MC ON MO.ClusterID = MC.ClusterID
	INNER JOIN MST_Cities MCT ON MC.CityID = MCT.CityID
	INNER JOIN [dbo].[Link_Menu_Outlet] LMO on LMO.OutletId = Mo.OutletId and Menuid = (CASE WHEN @MenuId = 0 THEN MenuId ELSE @MenuId END)          
	INNER JOIN [dbo].[Um_Mst_UserAccess] UMU on UMU.AccessId = LMO.AccessId and Userid =  (CASE WHEN @UserId IS NULL THEN UserId ELSE @UserId END)
	WHERE	--MO.IsActive = 1 AND	
			MC.CityID =  (CASE WHEN ISNULL(@cityId, 0) = 0 THEN MC.CityID ELSE @cityId END)
			AND MO.ClusterID = (CASE WHEN ISNULL(@clusterId, 0) = 0 THEN MO.ClusterID ELSE @clusterId END)
			AND OutletCode = (CASE WHEN ISNULL(@branchCode , '0') = '0' THEN OutletCode ELSE @branchCode END)  

	;WITH 
	/*CTE_DISTINCTACCOUNTNAME AS(
			SELECT DISTINCT ACCOUNTNAME FROM Rista_SaleItems WHERE AccountName NOT IN ('Food Sale', 'Beverage Sale', 'Liquor Sale')
			), */
	CTE_SALE AS (
			SELECT
				TDR.WeekNo [DATE],
				SUM(SIT.NetAmount) AS NETAMOUNT,
				0	BudgetAmount
			FROM  Rista_SaleInvoices (NOLOCK) SI
			INNER JOIN Rista_SaleItems (NOLOCK) SIT ON SIT.InvoiceID = SI.InvoiceID  
			--INNER JOIN CTE_DISTINCTACCOUNTNAME CD ON SIT.AccountName = CD.AccountName
			INNER JOIN Transaction_DateRange (NOLOCK) TDR ON SI.[InvoiceDay] = TDR.[Date] 
			INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
			WHERE SI.InvoiceDay BETWEEN @Startdt AND @Enddt AND SIT.AccountName NOT IN ('Food Sale', 'Beverage Sale', 'Liquor Sale')
			GROUP BY TDR.WeekNo
		),
	CTE_BUDGET AS (
			SELECT TDR.WeekNo [DATE], 0 NETAMOUNT, SUM(SBD.CategoryAmount) BudgetAmount 
			FROM Transaction_SalesBudgetDetails SBD
			INNER JOIN MST_SalesCategory MSC ON SBD.SalesCategoryID = MSC.SalesCategoryID
			--INNER JOIN CTE_DISTINCTACCOUNTNAME CD ON MSC.SalesCategory = CD.AccountName
			INNER JOIN Transaction_SalesBudget SB ON SB.SalesBudgetID = SBD.SalesBudgetID
			INNER JOIN Transaction_DateRange (NOLOCK) TDR ON SBD.[Date] = TDR.[Date] 
			INNER JOIN @OUTLETS MO ON SB.OutletID = MO.OutletID
			WHERE SBD.[Date] BETWEEN @Startdt AND @Enddt AND MSC.SalesCategory NOT IN ('Food Sale', 'Beverage Sale', 'Liquor Sale')
			GROUP BY TDR.WeekNo
		)
		
	SELECT DISTINCT TDR.WeekNo [DATE], ISNULL(S.NETAMOUNT, 0) NETAMOUNT, ISNULL(B.BudgetAmount, 0) BudgetAmount
	FROM Transaction_DateRange TDR 
	LEFT JOIN CTE_SALE S ON TDR.WeekNo = S.[DATE]
	FULL OUTER JOIN CTE_BUDGET B ON TDR.WeekNo = B.[DATE]
	WHERE TDR.Date BETWEEN @Startdt AND @Enddt
	AND (((SELECT COUNT(1) FROM CTE_SALE  WHERE [DATE] IS NOT NULL) > 0) OR ((SELECT COUNT(1) FROM CTE_BUDGET WHERE [DATE] IS NOT NULL) > 0))
END
GO
