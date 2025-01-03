USE [BELLONA_LIVE]
GO
/****** Object:  StoredProcedure [dbo].[dbsp_GetLast12Weeks_GrossProfit]    Script Date: 29-10-2024 00:14:14 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER    PROCEDURE [dbo].[dbsp_GetLast12Weeks_GrossProfit]
(@WEEK VARCHAR(20), @FINANCIALYEAR VARCHAR(10), @branchCode varchar(20) = null, @clusterId int = 0, @cityId int = 0,
@UserId uniqueIdentifier = null, @MenuId   Int= 0)
AS
BEGIN
	/*
		EXEC [dbo].[dbsp_GetLast12Weeks_GrossProfit] @WEEK = 'WEEK 30', @FINANCIALYEAR = '2024-25',  @branchCode = 'bnkrisha'
	*/

	--DECLARE @WEEK VARCHAR(20) = 'WEEK 27', @FINANCIALYEAR VARCHAR(10) = '2024-25',  @branchCode varchar(20) = 'bnkrisha', @clusterId int = 0, @cityId int = 0 
	DECLARE @Startdt DATETIMEOFFSET, @Enddt  DATETIMEOFFSET
	DECLARE @OUTLETS TABLE(OutletId INT, BRANCHCODE VARCHAR(20), BRANCHNAME VARCHAR(255), CLUSTERNAME VARCHAR(255), CityName VARCHAR(100))

	IF @branchCode = '0' OR @branchCode = 'null' or @branchCode = '' OR LTRIM(RTRIM(@branchCode)) = ''  SET @branchCode = NULL

	SELECT @Enddt = MAX([DATE]) FROM Transaction_DateRange WHERE WeekNo = @WEEK AND FinancialYear = @FINANCIALYEAR;
	--Start date from 12 week prior
	SELECT @Startdt = DATEADD(WEEK, -12, DATEADD(WEEK, DATEDIFF(WEEK, 0, @Enddt), 0));
	
	INSERT @OUTLETS (OutletId, BRANCHCODE, BRANCHNAME, CLUSTERNAME, CityName)
	SELECT distinct MO.OutletId, OutletCode BRANCHCODE, OutletName BRANCHNAME, MC.ClusterName, MCT.CityName
	FROM MST_OUTLET MO 
	INNER JOIN MST_Clusters MC ON MO.ClusterID = MC.ClusterID
	INNER JOIN MST_Cities MCT ON MC.CityID = MCT.CityID
	INNER JOIN [dbo].[Link_Menu_Outlet] LMO on LMO.OutletId = Mo.OutletId and Menuid = (CASE WHEN @MenuId = 0 THEN MenuId ELSE @MenuId END)          
	INNER JOIN [dbo].[Um_Mst_UserAccess] UMU on UMU.AccessId = LMO.AccessId and Userid =  (CASE WHEN @UserId IS NULL THEN UserId ELSE @UserId END)
	WHERE	MC.CityID =  (CASE WHEN ISNULL(@cityId, 0) = 0 THEN MC.CityID ELSE @cityId END)
			AND MO.ClusterID = (CASE WHEN ISNULL(@clusterId, 0) = 0 THEN MO.ClusterID ELSE @clusterId END)
			AND OutletCode = (CASE WHEN ISNULL(@branchCode , '0') = '0' THEN OutletCode ELSE @branchCode END) ;

	;WITH CTE_SALE AS (
			SELECT
				TDR.WeekNo [DATE],
				SUM(SI.NetAmount) AS NETAMOUNT,
				0	ExpenseAmount
			FROM  Rista_SaleInvoices (NOLOCK) SI
			INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
			INNER JOIN Transaction_DateRange (NOLOCK) TDR ON SI.InvoiceDay = TDR.[Date] 
			WHERE SI.InvoiceDay BETWEEN @Startdt AND @Enddt
			GROUP BY TDR.WeekNo
		),
	CTE_Expense AS (
			SELECT TDR.WeekNo [DATE], 0 NETAMOUNT, SUM(TDE.D_ProductionCost_Total) ExpenseAmount 
			FROM Transaction_Daily_Expense  TDE
			INNER JOIN  Transaction_DateRange TDR ON TDE.WeekNo = TDR.Week_number AND TDE.ExpenseYear = TDR.[Year] AND TDE.ExpenseMonth = TDR.[Month] 
			INNER JOIN @OUTLETS MO ON TDE.OutletId = MO.OutletID
			WHERE TDR.[WeekNo] = @WEEK and TDR.[FinancialYear] = @FINANCIALYEAR
			GROUP BY TDR.WeekNo
		)
	SELECT DISTINCT TDR.WeekNo [DATE], (ISNULL(S.NETAMOUNT, 0) - ISNULL(B.ExpenseAmount, 0)) [Value], ((ISNULL(S.NETAMOUNT, 0) - ISNULL(B.ExpenseAmount, 0))/NULLIF(S.NETAMOUNT, 0)) * 100 [PERCENTAGE]
	FROM Transaction_DateRange TDR 
	LEFT JOIN CTE_SALE S ON TDR.WeekNo = S.[DATE]
	FULL OUTER JOIN CTE_Expense B ON S.[DATE] = B.[DATE]
	WHERE TDR.Date BETWEEN @Startdt AND @Enddt
		AND (((SELECT COUNT(1) FROM CTE_SALE) > 0) OR ((SELECT COUNT(1) FROM CTE_Expense) > 0))
	
	
END
