select count(1)  FROM Rista_SaleSourceInfo (nolock) A INNER JOIN Rista_SaleInvoices (nolock) SI ON SI.InvoiceID = A.InvoiceID and SI.invoiceday = '2024-09-03';
select count(1)  FROM Rista_SaleTaxes (nolock) A INNER JOIN Rista_SaleInvoices (nolock) SI ON SI.InvoiceID = A.InvoiceID and SI.invoiceday = '2024-09-03';
select count(1)FROM Rista_SaleCharges (nolock) A INNER JOIN Rista_SaleInvoices (nolock) SI ON SI.InvoiceID = A.InvoiceID and SI.invoiceday = '2024-09-03';
select count(1)FROM [dbo].[Rista_SaleCustomers] (nolock) A INNER JOIN Rista_SaleInvoices (nolock) SI ON SI.InvoiceID = A.InvoiceID and SI.invoiceday = '2024-09-03';
select count(1)FROM [dbo].[Rista_SaleDelivery] (nolock) A INNER JOIN Rista_SaleInvoices (nolock) SI ON SI.InvoiceID = A.InvoiceID and SI.invoiceday = '2024-09-03';
select count(1)FROM [dbo].[Rista_SaleDeliveryBy] (nolock) A INNER JOIN Rista_SaleInvoices (nolock) SI ON SI.InvoiceID = A.InvoiceID and SI.invoiceday = '2024-09-03';
select count(1)FROM [dbo].[Rista_SaleDiscounts] (nolock) A INNER JOIN Rista_SaleInvoices (nolock) SI ON SI.InvoiceID = A.InvoiceID and SI.invoiceday = '2024-09-03';
select count(1)FROM [dbo].[Rista_SaleInvoiceItems] (nolock) A INNER JOIN Rista_SaleInvoices (nolock) SI ON SI.InvoiceID = A.InvoiceID and SI.invoiceday = '2024-09-03';
select count(1)FROM [dbo].[Rista_SaleItems] (nolock) A INNER JOIN Rista_SaleInvoices (nolock) SI ON SI.InvoiceID = A.InvoiceID and SI.invoiceday = '2024-09-03';
select count(1)FROM [dbo].[Rista_SaleLoyalties] (nolock) A INNER JOIN Rista_SaleInvoices (nolock) SI ON SI.InvoiceID = A.InvoiceID and SI.invoiceday = '2024-09-03';
select count(1)FROM [dbo].[Rista_SaleRefunds] (nolock) A INNER JOIN Rista_SaleInvoices (nolock) SI ON SI.InvoiceID = A.InvoiceID and SI.invoiceday = '2024-08-01'


select A.* FROM [dbo].[Rista_SaleItemOptions] (nolock) A 
INNER JOIN Rista_SaleItems (nolock) SIT ON Sit.ItemID = A.ItemID
INNER JOIN Rista_SaleInvoices (nolock) SI ON SI.InvoiceID = SIT.InvoiceID
WHERE SI.invoiceday = '2024-08-01'
select A.* FROM [dbo].[Rista_SaleItemTaxes] (nolock) A 
INNER JOIN Rista_SaleItems SIT (nolock) ON Sit.ItemID = A.ItemID
INNER JOIN Rista_SaleInvoices (nolock) SI ON SI.InvoiceID = SIT.InvoiceID
WHERE SI.invoiceday = '2024-08-01'
select A.* FROM [dbo].[Rista_SaleItemDiscounts] (nolock) A 
INNER JOIN Rista_SaleItems SIT (nolock) ON Sit.ItemID = A.ItemID
INNER JOIN Rista_SaleInvoices (nolock) SI ON SI.InvoiceID = SIT.InvoiceID
WHERE SI.invoiceday = '2024-08-01'

/*** To Get Record count for each Invoice and branchCode 

select 
		a.invoiceday, a.branchcode, count(distinct a.invoiceid) invoices, 
		count(c.invoiceid) sourceinfo, count(m.invoiceid) resourceinfo, count(e.invoiceid) Customers, count(h.invoiceid) Delivery, count(i.invoiceid) DeliveryBy,
		count(d.invoiceid) Charges, count(f.invoiceid) Taxes, count(g.invoiceid) Payments, count(j.invoiceid) Discounts, 
		count(k.invoiceid) Loyality, count(l.invoiceid) Refunds, count(n.invoiceid) Batches, count(O.invoiceid) EventLog,
		count(b.invoiceid) Items, count(b1.itemid) ItemDiscount, count(b2.itemid) ItemOptions, count(b3.itemid) ItemTaxes

from Rista_SaleInvoices a (nolock)

left join Rista_SaleSourceInfo c (nolock) on a.InvoiceID = c.InvoiceID
left join [Rista_SaleResource] m (nolock) on a.InvoiceID = m.InvoiceID
left join Rista_saleCustomers e (nolock) on a.InvoiceID = e.InvoiceID
left join [Rista_SaleDelivery] h (nolock) on a.InvoiceID = h.InvoiceID
left join [Rista_SaleDeliveryBy] i (nolock) on a.InvoiceID = i.InvoiceID

left join Rista_SaleCharges d (nolock) on a.InvoiceID = d.InvoiceID
left join Rista_SaleTaxes f (nolock) on a.InvoiceID = f.InvoiceID
left join [Rista_SalePayments] g (nolock) on a.InvoiceID = g.InvoiceID
left join [Rista_SaleDiscounts] j (nolock) on a.InvoiceID = j.InvoiceID
left join [Rista_SaleLoyalties] k (nolock) on a.InvoiceID = k.InvoiceID
left join [Rista_SaleRefunds] l (nolock) on a.InvoiceID = l.InvoiceID
left join [Rista_SaleBatches] n (nolock) on a.InvoiceID = n.InvoiceID
left join [Rista_SaleEventLog] o (nolock) on a.InvoiceID = o.InvoiceID

left join Rista_SaleItems b (nolock) on a.InvoiceID = b.InvoiceID
left join Rista_SaleItemDiscounts b1 (nolock) on a.InvoiceID = b.InvoiceID and b.itemid = b1.itemid
left join Rista_SaleItemOptions b2 (nolock) on a.InvoiceID = b.InvoiceID and b.itemid = b2.itemid
left join Rista_SaleItemTaxes b3 (nolock) on a.InvoiceID = b.InvoiceID and b.itemid = b3.itemid

group by a.invoiceday, a.branchcode
order by a.invoiceday, a.branchcode

*****/





/**** Sales Summary  */
DECLARE @Startdt datetime, @Enddt  datetime
set @Startdt = '2024-08-01'
set @Enddt = '2024-08-26';

WITH SALESSUMMARY([Sales Return], [Discounts], [Packaging charges (Direct Charges)],
				  [Net Sales], [Service charges (Indirect Charges)], [Taxes],
				  [Rounding], [Tip], [No. of people (PAX)])
AS (
		SELECT	SUM(ISNULL(SR.RefundAmount,0)) [Sales Return],
				SUM(ISNULL(SI.TotalDiscountAmount,0)) [Discounts],
				SUM(ISNULL(SI.directchargeamout,0)) [Packaging charges (Direct Charges)],
				SUM(ISNULL(SI.NetAmount,0))  [Net Sales],
				SUM(ISNULL(SI.NetChargeAmount,0)) [Service charges (Indirect Charges)],
				SUM(ISNULL(SI.TaxAmountExcluded,0)) [Taxes],
				SUM(ISNULL(si.RoundOffAmount,0)) [Rounding],
				SUM(ISNULL(si.TipAmount,0)) [Tip],
				SUM(ISNULL(si.PersonCount,0)) [No. of people (PAX)]
		FROM Rista_SaleInvoices SI
		LEFT JOIN Rista_SaleRefunds SR ON SI.InvoiceID = SR.InvoiceID
		/*LEFT JOIN Rista_SaleTaxes ST on SI.InvoiceID = ST.InvoiceID*/
		WHERE InvoiceDay BETWEEN @Startdt and @Enddt
		/*GROUP BY SI.InvoiceID */
	)
SELECT  [Net Sales] - [Packaging charges (Direct Charges)] + [Discounts] + [Sales Return] AS [Gross Sales],
		[Sales Return], [Discounts], [Packaging charges (Direct Charges)],
		[Net Sales], [Service charges (Indirect Charges)], [Taxes],
		[Rounding], [Tip], [No. of people (PAX)]
FROM SALESSUMMARY


/**** PAYMENT Summary  */
DECLARE @Startdt datetime, @Enddt  datetime
set @Startdt = '2024-08-01'
set @Enddt = '2024-08-26';

WITH PAYMENTSUMMARY([Payment Mode], [Amount])
AS (
		SELECT	ISNULL(SP.MODE, 'Payment Summary') [Payment Mode], 
				SUM(SP.Amount) [Amount] 
		FROM Rista_SalePayments SP
		INNER JOIN Rista_SaleInvoices SI ON SP.InvoiceID = SI.InvoiceID
		WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt
		GROUP BY SP.Mode with rollup
	)
SELECT  [Payment Mode], [Amount]
FROM PAYMENTSUMMARY


/**** Packaging charges (Direct Charges) - SUMMARY */
DECLARE @Startdt datetime, @Enddt  datetime
set @Startdt = '2024-08-01'
set @Enddt = '2024-08-26';

WITH DIRECTCHARGESUMMARY([Charge Type], [Amount])
AS (
		SELECT	ISNULL(SC.Name, 'Packaging charges (Direct Charges) - SUMMARY') [Charge Type], 
				SUM(SC.Amount) [Amount] 
		FROM Rista_SaleCharges Sc
		INNER JOIN Rista_SaleInvoices SI ON SC.InvoiceID = SI.InvoiceID
		WHERE  SC.IsDirectCharge = 1 AND  SI.InvoiceDay BETWEEN @Startdt and @Enddt
		GROUP BY SC.Name with rollup
	)
SELECT  [Charge Type], [Amount]
FROM DIRECTCHARGESUMMARY

/**** Service charges (Indirect Charges) - SUMMARY */
DECLARE @Startdt datetime, @Enddt  datetime
set @Startdt = '2024-08-01'
set @Enddt = '2024-08-26';

WITH INDIRECTCHARGESUMMARY([Charge Type], [Amount])
AS (
		SELECT	ISNULL(SC.Name, 'Service charges (Indirect Charges) - SUMMARY') [Charge Type], 
				SUM(SC.Amount) [Amount] 
		FROM Rista_SaleCharges Sc
		INNER JOIN Rista_SaleInvoices SI ON SC.InvoiceID = SI.InvoiceID
		WHERE  SC.IsDirectCharge = 0 AND  SI.InvoiceDay BETWEEN @Startdt and @Enddt
		GROUP BY SC.Name with rollup
	)
SELECT  [Charge Type], [Amount]
FROM INDIRECTCHARGESUMMARY

/*** TAX Summary **************/
DECLARE @Startdt datetime, @Enddt  datetime
set @Startdt = '2024-08-01'
set @Enddt = '2024-08-26';
WITH TaxDetails AS (
    SELECT 
        ST.name + ' ' + CAST(ST.percentage AS VARCHAR(5)) AS TaxType,
        SUM(ST.amount) AS TotalAmount,
        SUM(ST.saleAmount) AS TaxableAmount
    FROM Rista_SaleTaxes (nolock) ST
	INNER JOIN Rista_SaleInvoices (nolock) SI ON ST.InvoiceID = SI.InvoiceID
	WHERE  	SI.InvoiceDay BETWEEN @Startdt and @Enddt
			and  ST.name IN ('CGST', 'SGST')
    GROUP BY 
        ST.name, ST.percentage
)
SELECT 
    'Tax Summary' AS TaxType,
    SUM(TotalAmount) AS TotalAmount
FROM 
    TaxDetails

UNION ALL

SELECT 
    TaxType,
    TotalAmount
FROM 
    TaxDetails

UNION ALL

SELECT 
    TaxType + ' TaxableAmount' AS TaxType,
    TaxableAmount
FROM 
    TaxDetails;


/*** Channel Summary  ***/
DECLARE @Startdt datetime, @Enddt  datetime
set @Startdt = '2024-08-12'
set @Enddt = '2024-08-18'
SELECT 	RSI.branchCode, RSI.Channel, SUM(RSI.NetAmount) [Net Sales], 
		COUNT(1) [No. of Transactions], AVG(RSI.NetAmount) [Avg Sale Per Transaction],
		SUM(RSI.PersonCount) [No. of People], 
		SUM(RSI.NetAmount)/ SUM(CASE RSI.PersonCount WHEN 0 THEN 1 ELSE RSI.PersonCount END) [Avg Sale Per Person],
		SUM(RSI.DirectChargeAmout) [Packaging charges (Direct Charges)]
FROM Rista_SaleInvoices RSI
WHERE RSI.InvoiceDay BETWEEN @Startdt and @Enddt
GROUP BY RSI.branchCode, RSI.CHANNEL
ORDER BY RSI.branchCode, RSI.Channel


/** Transaction Summary ***/
DECLARE @Startdt datetime, @Enddt  datetime
set @Startdt = '2024-08-01'
set @Enddt = '2024-08-26';

SELECT	COUNT(1) [No. of Transactions],
		AVG(ISNULL(SI.NetAmount, 0))  [ Avg Sale Per Transaction],
		SUM(ISNULL(si.PersonCount,0)) [No. of people (PAX)],
		0 [Resource Capacity],
		0 [Capacity Utilization Ratio]
FROM Rista_SaleInvoices SI
WHERE InvoiceDay BETWEEN @Startdt and @Enddt

/**** Category Summary ******/
DECLARE @Startdt datetime, @Enddt  datetime
set @Startdt = '2024-08-12'
set @Enddt = '2024-08-18'

SELECT 	ISNULL(SIT.CategoryName, 'Category Summary') ,
		SUM(SIT.NetAmount) [Net Amount]
FROM Rista_SaleItems SIT INNER JOIN Rista_SaleInvoices SI ON SIT.InvoiceID = SI.InvoiceID
WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt
GROUP BY SIT.CategoryName WITH ROLLUP 
HAVING SUM(SIT.NetAmount) > 0


--OPTIONS 2
;WITH COLLECTIONS(CATEGORYNAME, NETAMOUNT) AS
(
	SELECT  LOWER(
				CASE 
					WHEN RIGHT(REPLACE(REPLACE(CategoryName, '-', ''), ' ', ''), 1) = 's' 
					THEN LEFT(REPLACE(REPLACE(CategoryName, '-', ''), ' ', ''), LEN(REPLACE(REPLACE(CategoryName, '-', ''), ' ', '')) - 1)
					ELSE REPLACE(REPLACE(CategoryName, '-', ''), ' ', '')
				END
			) AS NormalizedCategory,
			SUM(SIT.NetAmount) [Net Amount]
	FROM Rista_SaleItems SIT INNER JOIN Rista_SaleInvoices SI ON SIT.InvoiceID = SI.InvoiceID
	WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt
	GROUP BY SIT.CategoryName WITH ROLLUP 
	HAVING SUM(SIT.NetAmount) > 0
)
SELECT ISNULL(CATEGORYNAME, 'Category Summary'), SUM(NETAMOUNT)
FROM COLLECTIONS
GROUP BY CATEGORYNAME


/*** DIscount Summary  ***/
DECLARE @Startdt datetime, @Enddt  datetime
set @Startdt = '2024-08-12'
set @Enddt = '2024-08-18'

SELECT 	ISNULL(SD.Name, 'Discount Summary') ,
		SUM(SD.SaleAmount) [SaleAmount]
FROM Rista_SaleDiscounts SD INNER JOIN Rista_SaleInvoices SI ON SD.InvoiceID = SI.InvoiceID
WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt
GROUP BY SD.Name WITH ROLLUP 
HAVING SUM(SD.SaleAmount) > 0



/*** DIscount Summary Account wise ***/
DECLARE @Startdt datetime, @Enddt  datetime
set @Startdt = '2024-08-12'
set @Enddt = '2024-08-18'

SELECT 	ISNULL(SIT.AccountName, 'Discount Summary - Account wise') ,
		SUM(SD.SaleAmount) [SaleAmount]
FROM Rista_SaleItemDiscounts  SD 
INNER JOIN Rista_SaleItems SIT ON SD.ItemID = SIT.ItemID
INNER JOIN Rista_SaleInvoices SI ON Sit.InvoiceID = SI.InvoiceID
WHERE SI.InvoiceDay BETWEEN @Startdt and @Enddt
GROUP BY SIT.AccountName WITH ROLLUP 
HAVING SUM(SD.SaleAmount) > 0


/** Channel Summary *****/
DECLARE @Startdt datetime, @Enddt  datetime
set @Startdt = '2024-08-12'
set @Enddt = '2024-08-18'
SELECT  RSI.Channel, SUM(RSI.NetAmount) [Net Sales], 
		COUNT(1) [No. of Transactions], AVG(RSI.NetAmount) [Avg Sale Per Transaction],
		SUM(RSI.PersonCount) [No. of People], 
		SUM(RSI.NetAmount)/ SUM(CASE RSI.PersonCount WHEN 0 THEN 1 ELSE RSI.PersonCount END) [Avg Sale Per Person],
		SUM(RSI.DirectChargeAmout) [Packaging charges (Direct Charges)]
FROM Rista_SaleInvoices RSI
WHERE RSI.InvoiceDay BETWEEN @Startdt and @Enddt
GROUP BY RSI.CHANNEL
ORDER BY RSI.Channel


/*** Session Summary ****/
DECLARE @Startdt datetime, @Enddt  datetime
set @Startdt = '2024-08-12'
set @Enddt = '2024-08-18'
SELECT  RSI.SessionLabel, SUM(RSI.NetAmount) [Net Sales], 
		COUNT(1) [No. of Transactions], AVG(RSI.NetAmount) [Avg Sale Per Transaction],
		SUM(RSI.PersonCount) [No. of People], 
		SUM(RSI.NetAmount)/ SUM(CASE RSI.PersonCount WHEN 0 THEN 1 ELSE RSI.PersonCount END) [Avg Sale Per Person],
		SUM(RSI.DirectChargeAmout) [Packaging charges (Direct Charges)]
FROM Rista_SaleInvoices RSI
WHERE RSI.InvoiceDay BETWEEN @Startdt and @Enddt
GROUP BY RSI.SessionLabel
ORDER BY RSI.SessionLabel