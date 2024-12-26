USE BELLONA_LIVE;
GO
 
EXEC sp_spaceused;

USE BELLONA_LIVE;
GO
-- Shrink the mdf file
DBCC SHRINKFILE(N'USELDashboard', 0);
GO
-- Shrink the log.ldf file
DBCC SHRINKFILE(N'USELDashboard_log', 0);
GO

select * from sys.database_files