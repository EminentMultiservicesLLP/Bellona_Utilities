/*
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
*/

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
where a.InvoiceDay > '2024-08-31' --and a.InvoiceID = 
group by a.invoiceday, a.branchcode
order by a.invoiceday, a.branchcode

