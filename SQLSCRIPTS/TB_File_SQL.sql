/***
drop table TB_TrialBalance;
drop table TB_Particulars;
drop table TB_MISHead;
**/
go

CREATE TABLE TB_MISHead (
    head_id INT PRIMARY KEY IDENTITY(1,1),
    head_name VARCHAR(100) UNIQUE NOT NULL,
	nature varchar(20)
);
go
CREATE TABLE TB_Particulars (
	Id INT PRIMARY KEY IDENTITY(1,1),
    code VARCHAR(20),
	particulars VARCHAR(100) NOT NULL
);
go
CREATE TABLE TB_TrialBalance(
	tb_id INT PRIMARY KEY IDENTITY(1,1),
	fileName_uploaded varchar(255), 
	branch_id VARCHAR(20),
    head_id int,
	particulars_id int,
	tb_date DATETIMEOFFSET,
    tb_amount DECIMAL(18, 2) DEFAULT 0.0,
	FOREIGN KEY (head_id) REFERENCES TB_MISHead(head_id),
	FOREIGN KEY (particulars_id) REFERENCES TB_Particulars(Id)
)
go

CREATE TABLE error_log (
    id INTEGER PRIMARY KEY  IDENTITY(1,1),
	error_process varchar(25),
	fileName_uploaded varchar(255), 
    errorMessage varchar(max),
    rowNumber INTEGER,
	colNumber INTEGER,
	colName VARCHAR(25),
    error_time datetime DEFAULT current_timestamp
);
go

select * from Mst_Properties



SELECT * FROM TB_MISHead;
SELECT * FROM TB_Particulars;
SELECT * FROM TB_TrialBalance;

