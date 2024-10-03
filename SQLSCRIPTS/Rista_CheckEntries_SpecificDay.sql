DECLARE @INVOICEDAY VARCHAR(20);
SET @INVOICEDAY = '2024-09-28';

select count(distinct invoiceid) Invoice FROM Rista_SaleInvoices (nolock) WHERE InvoiceDay = @INVOICEDAY;

select count( distinct a.invoiceid) SalesItems FROM Rista_SaleItems (NOLOCK) A 
INNER JOIN Rista_SaleInvoices (NOLOCK) B ON   a.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

select count( distinct a.invoiceid) Rista_SalePayments FROM Rista_SalePayments (NOLOCK) A 
INNER JOIN Rista_SaleInvoices (NOLOCK) B ON   a.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

select count( distinct a.invoiceid) Rista_SaleSourceInfo FROM Rista_SaleSourceInfo (NOLOCK) A 
INNER JOIN Rista_SaleInvoices (NOLOCK) B ON   a.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

select count( distinct a.invoiceid) Rista_SaleInvoiceItems FROM Rista_SaleInvoiceItems (NOLOCK) A 
INNER JOIN Rista_SaleInvoices (NOLOCK) B ON   a.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

select count( distinct a.invoiceid) Rista_SaleDeliveryBy FROM Rista_SaleDeliveryBy (NOLOCK) A 
INNER JOIN Rista_SaleInvoices (NOLOCK) B ON   a.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

select count( distinct a.invoiceid) Rista_SaleDelivery FROM Rista_SaleDelivery (NOLOCK) A 
INNER JOIN Rista_SaleInvoices (NOLOCK) B ON   a.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

select count( distinct a.invoiceid) Rista_SaleSourceInfo FROM Rista_SaleSourceInfo (NOLOCK) A 
INNER JOIN Rista_SaleInvoices (NOLOCK) B ON   a.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

select count( distinct a.invoiceid) Rista_SaleCharges FROM Rista_SaleCharges (NOLOCK) A 
INNER JOIN Rista_SaleInvoices (NOLOCK) B ON   a.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

select count( distinct a.invoiceid) Rista_SaleDiscounts FROM Rista_SaleDiscounts (NOLOCK) A 
INNER JOIN Rista_SaleInvoices (NOLOCK) B ON   a.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;


select count( distinct a.invoiceid) Rista_SaleTaxes FROM Rista_SaleTaxes (NOLOCK) A 
INNER JOIN Rista_SaleInvoices (NOLOCK) B ON   a.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

select count( distinct a.invoiceid) Rista_SaleEventLog FROM Rista_SaleEventLog (NOLOCK) A 
INNER JOIN Rista_SaleInvoices (NOLOCK) B ON   a.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

select count( distinct a.invoiceid) Rista_SaleCustomers FROM Rista_SaleCustomers (NOLOCK) A 
INNER JOIN Rista_SaleInvoices (NOLOCK) B ON   a.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

select count( distinct a.invoiceid) Rista_SaleLoyalties FROM Rista_SaleLoyalties (NOLOCK) A 
INNER JOIN Rista_SaleInvoices (NOLOCK) B ON   a.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

select count( distinct a.ItemID) Rista_SaleItemDiscounts FROM Rista_SaleItemDiscounts (NOLOCK) A 
INNER JOIN Rista_SaleItems (NOLOCK) B ON   a.ItemID = B.ItemID
INNER JOIN Rista_SaleInvoices C ON B.InvoiceID = C.InvoiceID AND C.InvoiceDay = @INVOICEDAY;

select count( distinct a.ItemOptionID) Rista_SaleItemOptionsTaxes FROM Rista_SaleItemOptionsTaxes (NOLOCK) A 
INNER JOIN  Rista_SaleItemOptions (NOLOCK) B ON   a.ItemOptionID = B.ItemOptionID
INNER JOIN Rista_SaleItems C ON b.ItemID = C.ItemID
INNER JOIN Rista_SaleInvoices D ON C.InvoiceID = D.InvoiceID AND D.InvoiceDay = @INVOICEDAY;


select count( distinct a.ItemID) Rista_SaleItemTaxes FROM Rista_SaleItemTaxes (NOLOCK) A 
INNER JOIN Rista_SaleItems (NOLOCK) B ON   a.ItemID = B.ItemID
INNER JOIN Rista_SaleInvoices C ON B.InvoiceID = C.InvoiceID AND C.InvoiceDay = @INVOICEDAY;

select count( distinct a.ItemID) Rista_SaleItemTaxes FROM Rista_SaleItemTaxes (NOLOCK) A 
INNER JOIN Rista_SaleItems (NOLOCK) B ON   a.ItemID = B.ItemID
INNER JOIN Rista_SaleInvoices C ON B.InvoiceID = C.InvoiceID AND C.InvoiceDay = @INVOICEDAY;


select count( distinct a.invoiceid) SourceInfo FROM Rista_SaleSourceInfo (NOLOCK) A 
INNER JOIN Rista_SaleInvoices (NOLOCK) B ON   a.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

select count( distinct a.invoiceid) SalesResource FROM Rista_SaleResource (NOLOCK) A 
INNER JOIN Rista_SaleInvoices (NOLOCK) B ON   a.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;




/*
SELECT branchCode, @INVOICEDAY, SUM(A.NETAMOUNT)
FROM Rista_SaleInvoices A
LEFT JOIN Rista_SalePayments B ON A.InvoiceID = B.InvoiceID
WHERE A.InvoiceDay = @INVOICEDAY AND B.InvoiceID IS NULL AND InvoiceType <> 'NC' AND Status <> 'CANCELLED'  AND A.NetAmount > 0
*/