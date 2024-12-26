
USE [BELLONA_LIVE]
GO

IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='Payment_Invoice_INDX' AND object_id = OBJECT_ID('[dbo].[Rista_SalePayments]'))
BEGIN
	CREATE NONCLUSTERED INDEX [Payment_Invoice_INDX]
	ON [dbo].[Rista_SalePayments] ([InvoiceID])
	INCLUDE ([Mode],[Amount])
END
GO

IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='NCI_SALEBUDGET_ID_DATE' AND object_id = OBJECT_ID('[dbo].[Transaction_SalesBudgetDetails]'))
BEGIN
	CREATE NONCLUSTERED INDEX [NCI_SALEBUDGET_ID_DATE]
	ON [dbo].[Transaction_SalesBudgetDetails] ([SalesBudgetID],[Date])
	INCLUDE ([SalesCategoryID])
END
GO


IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='INDX_InvoiceId' AND object_id = OBJECT_ID('[dbo].[Rista_SaleItems]'))
BEGIN
	CREATE NONCLUSTERED INDEX [INDX_InvoiceId]
	ON [dbo].[Rista_SaleItems] ([InvoiceID])
	INCLUDE ([AccountName],[NetAmount])
END
go

IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='INDX_SaleSource_InvoiceId' AND object_id = OBJECT_ID('[dbo].[Rista_SaleSourceInfo]'))
BEGIN
	CREATE NONCLUSTERED INDEX [INDX_SaleSource_InvoiceId]
	ON [dbo].[Rista_SaleSourceInfo] ([InvoiceID])
END
Go


IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='INDX_SALE_INVOICE_BRANCH_DAY_TYPE_STATUS' AND object_id = OBJECT_ID('[dbo].[Rista_SaleInvoices]'))
BEGIN
	CREATE NONCLUSTERED INDEX INDX_SALE_INVOICE_BRANCH_DAY_TYPE_STATUS
	ON [dbo].[Rista_SaleInvoices] ([branchCode],[InvoiceDay],[InvoiceType],[Status])
END
Go

IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='INDX_SALE_INVOICEday_BRANCH_DAY_TYPE_STATUS' AND object_id = OBJECT_ID('[dbo].[Rista_SaleInvoices]'))
BEGIN
	CREATE NONCLUSTERED INDEX [INDX_SALE_INVOICEday_BRANCH_DAY_TYPE_STATUS]
	ON [dbo].[Rista_SaleInvoices] ([InvoiceDay])
	INCLUDE ([branchCode],[InvoiceType],[Status])
END
Go

IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='INDX_FINYEAR_WEEKNO' AND object_id = OBJECT_ID('[dbo].[Transaction_DateRange]'))
BEGIN
	CREATE NONCLUSTERED INDEX [INDX_FINYEAR_WEEKNO]
	ON [dbo].[Transaction_DateRange] ([FinancialYear],[WeekNo])
END
GO

IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='inx_Link_Menu_Outlet_outletid' AND object_id = OBJECT_ID('[dbo].[Link_Menu_Outlet]'))
BEGIN
	CREATE NONCLUSTERED INDEX [inx_Link_Menu_Outlet_outletid]
	ON [dbo].[Link_Menu_Outlet] ([OutletId])
	INCLUDE ([AccessId],[MenuId])
END
go

IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='Transaction_DateRange_date' AND object_id = OBJECT_ID('[Transaction_DateRange]'))
BEGIN
	CREATE NONCLUSTERED INDEX [Transaction_DateRange_date]
	ON [dbo].[Transaction_DateRange] ([Date])
	INCLUDE ([WeekNo])
END

GO