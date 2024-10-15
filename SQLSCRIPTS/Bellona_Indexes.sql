
USE [BELLONA_LIVE]
GO

IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='Payment_Invoice_INDX' AND object_id = OBJECT_ID('[dbo].[Rista_SalePayments]'))
BEGIN
	CREATE NONCLUSTERED INDEX [Payment_Invoice_INDX]
	ON [dbo].[Rista_SalePayments] ([InvoiceID])
END
GO

IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='INDX_InvoiceId' AND object_id = OBJECT_ID('[dbo].[Rista_SaleItems]'))
BEGIN
	CREATE NONCLUSTERED INDEX [INDX_InvoiceId]
	ON [dbo].[Rista_SaleItems] ([InvoiceID])
	INCLUDE ([AccountName],[NetAmount])
END
go

IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='INDX_SALE_INVOICE_BRANCH_DAY_TYPE_STATUS' AND object_id = OBJECT_ID('[dbo].[Rista_SaleInvoices]'))
BEGIN
	CREATE NONCLUSTERED INDEX INDX_SALE_INVOICE_BRANCH_DAY_TYPE_STATUS
	ON [dbo].[Rista_SaleInvoices] ([branchCode],[InvoiceDay],[InvoiceType],[Status])
END
Go

IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='INDX_FINYEAR_WEEKNO' AND object_id = OBJECT_ID('[dbo].[Transaction_DateRange]'))
BEGIN
	CREATE NONCLUSTERED INDEX [INDX_FINYEAR_WEEKNO]
	ON [dbo].[Transaction_DateRange] ([FinancialYear],[WeekNo])
END
GO