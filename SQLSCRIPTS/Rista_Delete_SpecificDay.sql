DECLARE @INVOICEDAY VARCHAR(20);
SET @INVOICEDAY = '2024-09-22';

DELETE A FROM Rista_SaleSourceInfo A
INNER JOIN Rista_SaleInvoices B ON A.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

DELETE A FROM Rista_SaleInvoiceItems A
INNER JOIN Rista_SaleInvoices B ON A.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

DELETE A FROM Rista_SaleDeliveryBy A
INNER JOIN Rista_SaleInvoices B ON A.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

DELETE A FROM Rista_SaleDelivery A
INNER JOIN Rista_SaleInvoices B ON A.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

DELETE A FROM Rista_SaleSourceInfo A
INNER JOIN Rista_SaleInvoices B ON A.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

DELETE A FROM Rista_SaleCharges A
INNER JOIN Rista_SaleInvoices B ON A.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

DELETE A FROM Rista_SaleDiscounts A
INNER JOIN Rista_SaleInvoices B ON A.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

DELETE A FROM Rista_SalePayments A
INNER JOIN Rista_SaleInvoices B ON A.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

DELETE A FROM Rista_SaleTaxes A
INNER JOIN Rista_SaleInvoices B ON A.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

DELETE A FROM Rista_SaleEventLog A
INNER JOIN Rista_SaleInvoices B ON A.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

DELETE A FROM Rista_SaleCustomers A
INNER JOIN Rista_SaleInvoices B ON A.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

DELETE A FROM Rista_SaleLoyalties A
INNER JOIN Rista_SaleInvoices B ON A.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;

DELETE A FROM Rista_SaleItemDiscounts A
INNER JOIN Rista_SaleItems B ON A.ItemID = B.ItemID
INNER JOIN Rista_SaleInvoices C ON B.InvoiceID = C.InvoiceID AND C.InvoiceDay = @INVOICEDAY;

DELETE A FROM Rista_SaleItemOptionsTaxes A
INNER JOIN  Rista_SaleItemOptions B ON A.ItemOptionID = B.ItemOptionID
INNER JOIN Rista_SaleItems C ON b.ItemID = C.ItemID
INNER JOIN Rista_SaleInvoices D ON C.InvoiceID = D.InvoiceID AND D.InvoiceDay = @INVOICEDAY;


DELETE A FROM Rista_SaleItemOptions A
INNER JOIN Rista_SaleItems B ON A.ItemID = B.ItemID
INNER JOIN Rista_SaleInvoices C ON B.InvoiceID = C.InvoiceID AND C.InvoiceDay = @INVOICEDAY;

DELETE A FROM Rista_SaleItemTaxes A
INNER JOIN Rista_SaleItems B ON A.ItemID = B.ItemID
INNER JOIN Rista_SaleInvoices C ON B.InvoiceID = C.InvoiceID AND C.InvoiceDay = @INVOICEDAY;


DELETE A FROM Rista_SaleItems A
INNER JOIN Rista_SaleInvoices B ON A.InvoiceID = B.InvoiceID AND B.InvoiceDay = @INVOICEDAY;


DELETE FROM Rista_SaleInvoices WHERE InvoiceDay = @INVOICEDAY;
