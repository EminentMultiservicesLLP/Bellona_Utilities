USE [BELLONA_LIVE]
GO
/****** Object:  StoredProcedure [dbo].[dbsp_GetDSR_Comparision_Daywise]    Script Date: 15-10-2024 14:42:34 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER  PROCEDURE [dbo].[dbsp_GetLast12Weeks_SalesVsBudget]
(@WEEK VARCHAR(20), @FINANCIALYEAR VARCHAR(10), @branchCode varchar(20) = null, @clusterId int = 0, @cityId int = 0 )
AS
BEGIN
	/*
		EXEC [dbo].[dbsp_GetLast12Weeks_SalesVsBudget] @WEEK = 'WEEK 27', @FINANCIALYEAR = '2024-25',  @branchCode = 'bnkrisha'
	*/

	--DECLARE @WEEK VARCHAR(20) = 'WEEK 27', @FINANCIALYEAR VARCHAR(10) = '2024-25',  @branchCode varchar(20) = 'bnkrisha', @clusterId int = 0, @cityId int = 0 
	DECLARE @Startdt DATETIMEOFFSET, @Enddt  DATETIMEOFFSET
	DECLARE @OUTLETS TABLE(OutletId INT, BRANCHCODE VARCHAR(20), BRANCHNAME VARCHAR(255), CLUSTERNAME VARCHAR(255), CityName VARCHAR(100))

	IF @branchCode = '0' SET @branchCode = NULL

	SELECT @Enddt = MAX([DATE]) FROM Transaction_DateRange WHERE WeekNo = @WEEK AND FinancialYear = @FINANCIALYEAR;
	--Start date from 12 week prior
	SELECT @Startdt = DATEADD(WEEK, -12, DATEADD(WEEK, DATEDIFF(WEEK, 0, @Enddt), 0));
	
	INSERT @OUTLETS (OutletId, BRANCHCODE, BRANCHNAME, CLUSTERNAME, CityName)
	SELECT distinct OutletId, OutletCode BRANCHCODE, OutletName BRANCHNAME, MC.ClusterName, MCT.CityName
	FROM MST_OUTLET MO 
	INNER JOIN MST_Clusters MC ON MO.ClusterID = MC.ClusterID
	INNER JOIN MST_Cities MCT ON MC.CityID = MCT.CityID
	WHERE	MO.IsActive = 1 	
			AND MC.CityID =  (CASE WHEN ISNULL(@cityId, 0) = 0 THEN MC.CityID ELSE @cityId END)
			AND MO.ClusterID = (CASE WHEN ISNULL(@clusterId, 0) = 0 THEN MO.ClusterID ELSE @clusterId END)
			AND OutletCode = (CASE WHEN ISNULL(@branchCode , '0') = '0' THEN OutletCode ELSE @branchCode END) ;

	;WITH CTE_SALE AS (
			SELECT
				SI.InvoiceDay [DATE],
				SUM(SI.NetAmount) AS NETAMOUNT,
				0	BudgetAmount
			FROM  Rista_SaleInvoices (NOLOCK) SI
			INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
			WHERE SI.InvoiceDay BETWEEN @Startdt AND @Enddt
			GROUP BY SI.InvoiceDay
		),
	CTE_BUDGET AS (
			SELECT SBD.[DATE] [DATE], 0 NETAMOUNT, SUM(SBD.CategoryAmount) BudgetAmount 
			FROM Transaction_SalesBudgetDetails SBD
			INNER JOIN Transaction_SalesBudget SB ON SB.SalesBudgetID = SBD.SalesBudgetID
			INNER JOIN @OUTLETS MO ON SB.OutletID = MO.OutletId
			WHERE SBD.[Date] BETWEEN @Startdt AND @Enddt
			GROUP BY SBD.[DATE]
		)
	SELECT S.[DATE], ISNULL(S.NETAMOUNT, 0) NETAMOUNT, ISNULL(B.BudgetAmount, 0) BudgetAmount
	FROM CTE_SALE S
	FULL OUTER JOIN CTE_BUDGET B ON S.[DATE] = B.[DATE];
	
END
GO





