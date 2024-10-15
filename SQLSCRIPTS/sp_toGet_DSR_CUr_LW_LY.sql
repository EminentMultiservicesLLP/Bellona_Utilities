
-- DATE, DAY, WEEK WILL BE PASSED
-- CURRENT DAY DATA NEE TO SHOW
DECLARE @WEEK VARCHAR(20) = 'WEEK 27', @FINANCIALYEAR VARCHAR(10) = '2024-25',  @branchCode varchar(20) = 'bnahisha', @clusterId int = 0, @cityId int = 0 
DECLARE @Startdt DATETIMEOFFSET, @Enddt  DATETIMEOFFSET
DECLARE @OUTLETS TABLE(BRANCHCODE VARCHAR(20), BRANCHNAME VARCHAR(255), CLUSTERNAME VARCHAR(255), CityName VARCHAR(100))
DECLARE @cols AS NVARCHAR(250), @colsNullhandling NVARCHAR(250), @query AS NVARCHAR(MAX);

IF OBJECT_ID('tempdb..#COVER_TEMP') IS NOT NULL DROP TABLE #COVER_TEMP;
IF @branchCode = '0' SET @branchCode = NULL

--SELECT * FROM Transaction_DateRange WHERE WeekNo = 'WEEK 27'
SELECT @Startdt= MIN([DATE]), @Enddt = MAX([DATE]) FROM Transaction_DateRange WHERE WeekNo = @WEEK AND FinancialYear = @FINANCIALYEAR

SELECT @Startdt, @Enddt

INSERT @OUTLETS (BRANCHCODE, BRANCHNAME, CLUSTERNAME, CityName)
SELECT distinct OutletCode BRANCHCODE, OutletName BRANCHNAME, MC.ClusterName, MCT.CityName
FROM MST_OUTLET MO 
INNER JOIN MST_Clusters MC ON MO.ClusterID = MC.ClusterID
INNER JOIN MST_Cities MCT ON MC.CityID = MCT.CityID
WHERE	MC.CityID =  (CASE WHEN ISNULL(@cityId, 0) = 0 THEN MC.CityID ELSE @cityId END)
		AND MO.ClusterID = (CASE WHEN ISNULL(@clusterId, 0) = 0 THEN MO.ClusterID ELSE @clusterId END)
		AND OutletCode = (CASE WHEN ISNULL(@branchCode , '0') = '0' THEN OutletCode ELSE @branchCode END)  

SELECT
	SI.InvoiceDay,
	MST.SessionName,
	MAX(SI.PersonCount) AS COVERS
INTO #COVER_TEMP
FROM  MST_SaleTimeSessions (NOLOCK) MST
LEFT JOIN Rista_SaleInvoices (NOLOCK) SI ON CAST(SI.InvoiceDate AS TIME) BETWEEN MST.StartTime AND MST.EndTime
INNER JOIN @OUTLETS MO ON SI.branchCode = MO.BRANCHCODE AND SI.InvoiceType <> 'NC' AND SI.Status <> 'Cancelled'
WHERE SI.InvoiceDay BETWEEN @Startdt AND @Enddt
GROUP BY MST.SessionName, SI.InvoiceDay



SELECT @cols = STRING_AGG(''''+ SessionName, ''',') WITHIN GROUP (ORDER BY starttime) FROM MST_SaleTimeSessions;
SELECT @colsNullhandling = STRING_AGG('['+[InvoiceDay], '],') WITHIN GROUP (ORDER BY InvoiceDay)
FROM (
    SELECT DISTINCT CONVERT(VARCHAR, [InvoiceDay], 120) AS [InvoiceDay] FROM #COVER_TEMP WHERE [InvoiceDay] IS NOT NULL 
) AS DistinctInvoiceDays;

SELECT * FROM #COVER_TEMP
select @cols, @colsNullhandling
--SELECT * FROM MST_SaleTimeSessions ORDER BY STARTTIME
-- Now pivot the data to have InvoiceDay as columns
SET @query = '
	SELECT 
		SessionName, '+ @colsNullhandling +']
	FROM 
		#COVER_TEMP
	PIVOT (
		SUM(Covers)
		FOR SessionName IN (' + @cols + ')
	) AS PivotedData
	'
	
PRINT(@query)

EXEC sp_executesql @query;


