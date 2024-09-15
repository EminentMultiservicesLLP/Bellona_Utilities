ALTER TABLE Mst_Properties
ADD ReadDataOnSameLine bit DEFAULT 1
go

UPDATE Mst_Properties
SET ReadDataOnSameLine = 1
go

UPDATE Mst_Properties
SET ReadDataOnSameLine = 0
WHERE PropertyName IN('SupplierName','OutletName')
GO


CREATE or ALTER  PROCEDURE [dbo].[dbsp_SelectAllProperties] 
AS BEGIN     
	SET NOCOUNT ON;      
	SELECT SearchSequence, PropertyId, PropertyName, PropertyStart, PropertyEnd, LineLimit, SearchFromStart,  ReadDataOnSameLine   
	FROM dbo.Mst_Properties  
	WHERE Deactive = 0
	ORDER BY SearchSequence ASC; 
END
GO

 

CREATE OR ALTER  PROCEDURE [dbo].[dbsp_InsertDMSPropertiesValues_]  
(@DocumentId INT, @PropertyId INT, @PropertyContent VARCHAR(MAX))
AS  
BEGIN  
    SET NOCOUNT ON;  
  
    INSERT INTO dbo.DMS_Properties_Value (DocumentId, PropertyId, PropertyContent)  
    VALUES(@DocumentId, @PropertyId, @PropertyContent);
END  
GO