CREATE PROCEDURE dbo.dbsp_GetDSRDetails
(	@Startdt datetime, @Enddt  datetime )
AS
BEGIN
	set @Startdt = '2024-08-12'
	set @Enddt = '2024-08-18'

	/** Get Sale Main Category details *******/
	SELECT 	
		SUM(CASE WHEN AccountName = 'Food Sale' THEN SIT.NetAmount ELSE 0 END) AS 'FOOD SALE',
		SUM(CASE WHEN AccountName = 'Beverage Sale' THEN SIT.NetAmount ELSE 0 END) AS 'BEVERAGE SALE',
		SUM(CASE WHEN AccountName = 'LIQUOR SALE' THEN SIT.NetAmount ELSE 0 END) AS 'LIQUOR SALE',
		SUM(CASE WHEN AccountName = 'TOBACO SALE' THEN SIT.NetAmount ELSE 0 END) AS 'TOBACO SALE',
		SUM(CASE WHEN AccountName NOT IN ('Food Sale', 'Beverage Sale', 'TOBACO SALE', 'LIQUOR SALE') THEN SIT.NetAmount ELSE 0 END) AS 'OTHER SALE',
		SUM(SIT.NetDiscountAmount) as 'DISCOUNT AMOUNT',
		SUM(SI.NetChargeAmount) as 'SERVICE CHARGE AMOUNT',
		SUM(SI.NetAmount) AS 'SALES NET TOTAL',
		SUM(SI.TotalAmount) AS 'SALES TOTAL WITH SC'
	FROM Rista_SaleItems SIT INNER JOIN Rista_SaleInvoices SI ON SIT.InvoiceID = SI.InvoiceID
	WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt
	

END
GO