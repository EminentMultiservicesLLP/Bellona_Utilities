/** Check DB Size **/
SELECT DB_NAME(database_id), name AS LogicalName, physical_name AS PhysicalName, type_desc AS FileType, size DB_SIZE
FROM sys.master_files
order by size desc



USE BELLONA_LIVE;
GO
-- Truncate the log by changing the database recovery model to SIMPLE.
ALTER DATABASE BELLONA_LIVE
SET RECOVERY SIMPLE;
GO
-- Shrink the truncated log file to 1 MB.
DBCC SHRINKFILE (BELLONA_LIVE_log, 1);
GO
-- Reset the database recovery model.
ALTER DATABASE BELLONA_LIVE
SET RECOVERY FULL;
GO




/*** Change DB logical name 
ALTER DATABASE MECHCON_LIVE
SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
GO

ALTER DATABASE MECHCON_LIVE
MODIFY FILE (NAME = 'BISERPLive', NEWNAME = 'MECHCON_LIVE_DATA');
go
ALTER DATABASE MECHCON_LIVE
MODIFY FILE (NAME = 'BISERPLive_log', NEWNAME = 'MECHCON_LIVE_LOG');
go


ALTER DATABASE Bellona_Dev
SET MULTI_USER;
GO
*/