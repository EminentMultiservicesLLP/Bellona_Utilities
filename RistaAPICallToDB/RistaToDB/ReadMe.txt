/*** RUn Below SQL statement before running Python program **/
-- Step 1
alter table MST_OUTLET_U alter column OutletCode varchar(20);
GO

--Step 2
UPDATE MST_OUTLET_U SET OutletCode = 'bnwkfyle' WHERE OutletName = 'FYOLE MOM WAKAD PUNE'
UPDATE MST_OUTLET_U SET OutletCode = 'bnwkeght' WHERE OutletName = 'EIGHT MOM WAKAD PUNE'
UPDATE MST_OUTLET_U SET OutletCode = 'bnmafylg' WHERE OutletName = 'FYOLE UG MOA BANGALORE'
UPDATE MST_OUTLET_U SET OutletCode = 'bnmadoba' WHERE OutletName = 'DOBARAA MOA BANGALORE'
UPDATE MST_OUTLET_U SET OutletCode = 'bnmaeght' WHERE OutletName = 'EIGHT MOA BANGALORE'
UPDATE MST_OUTLET_U SET OutletCode = 'bnmaisha' WHERE OutletName = 'ISHAARA - MOA BANGALORE'
UPDATE MST_OUTLET_U SET OutletCode = 'bnkrdobb' WHERE OutletName = 'DOBARAA PMC KURLA'
UPDATE MST_OUTLET_U SET OutletCode = 'bnkrisha' WHERE OutletName = 'ISHAARA - PMC KURLA'
UPDATE MST_OUTLET_U SET OutletCode = 'bnvnalra' WHERE OutletName = 'CAFFE ALLORA MOM PUNE'
UPDATE MST_OUTLET_U SET OutletCode = 'bnvnchch' WHERE OutletName = 'CHA PMC PUNE'
UPDATE MST_OUTLET_U SET OutletCode = 'bnvndoba' WHERE OutletName = 'DOBARAA PMC PUNE'
UPDATE MST_OUTLET_U SET OutletCode = 'bnvnisha' WHERE OutletName = 'ISHAARA PMC PUNE'
UPDATE MST_OUTLET_U SET OutletCode = 'bnbgalra' WHERE OutletName = 'CAFFE ALLORA PMC BANGALORE'
UPDATE MST_OUTLET_U SET OutletCode = 'bnbgchch' WHERE OutletName = 'CHA PMC BANGALORE'
UPDATE MST_OUTLET_U SET OutletCode = 'bnbgdoba' WHERE OutletName = 'DOBARAA PMC BANGALORE'
UPDATE MST_OUTLET_U SET OutletCode = 'bnbgisha' WHERE OutletName = 'ISHAARA - PMC BANGALORE'
UPDATE MST_OUTLET_U SET OutletCode = 'bnahchcf' WHERE OutletName = 'CHA AHMEDABAD PALLEDIUM'
UPDATE MST_OUTLET_U SET OutletCode = 'bnahpout' WHERE OutletName = 'POULT AHMEDABAD'
UPDATE MST_OUTLET_U SET OutletCode = 'bnahisha' WHERE OutletName = 'ISHAARA AHMEDABAD'
UPDATE MST_OUTLET_U SET OutletCode = 'bnahalra' WHERE OutletName = 'CAFFE ALLORA AHMEDABAD PALLEDIUM'
UPDATE MST_OUTLET_U SET OutletCode = 'bnlpalra' WHERE OutletName = 'CAFFE ALLORA HSP MUMBAI'
UPDATE MST_OUTLET_U SET OutletCode = 'bnlpchcf' WHERE OutletName = 'CHA HSP MUMBAI'
UPDATE MST_OUTLET_U SET OutletCode = 'bnlplegm' WHERE OutletName = 'LEGUME HSP MUMBAI'
UPDATE MST_OUTLET_U SET OutletCode = 'bnlppout' WHERE OutletName = 'POULT HSP'
UPDATE MST_OUTLET_U SET OutletCode = 'bnlkeght' WHERE OutletName = 'EIGHT PALASIO LUCKNOW'
UPDATE MST_OUTLET_U SET OutletCode = 'bnlpeght' WHERE OutletName = 'EIGHT HSP MUMBAI'
UPDATE MST_OUTLET_U SET OutletCode = 'bnbgisha' WHERE OutletName = 'ISHAARA - PMC BANGALORE'
UPDATE MST_OUTLET_U SET OutletCode = 'bnlkisha' WHERE OutletName = 'ISHAARA PALLACIO LUCKNOW'
UPDATE MST_OUTLET_U SET OutletCode = 'bnlkdoba' WHERE OutletName = 'DOBARAA PALLACIO LUCKNOW'
UPDATE MST_OUTLET_U SET OutletCode = 'ISR-P' WHERE OutletName = 'ISHAARA PMC PUNE'
UPDATE MST_OUTLET_U SET OutletCode = 'DBR-P' WHERE OutletName = 'DOBARAA PMC PUNE'
UPDATE MST_OUTLET_U SET OutletCode = 'bnlpdoba' WHERE OutletName = 'DOBARAA PMC KURLA'
UPDATE MST_OUTLET_U SET OutletCode = 'bnlpjuls' WHERE OutletName = 'JULIUS HSP'
UPDATE MST_OUTLET_U SET OutletCode = 'bnlpjuls' WHERE OutletName = 'JULIUS HSP MUMBAI'
UPDATE MST_OUTLET_U SET OutletCode = 'bnlpfyle' WHERE OutletName = 'FYOLE HSP MUMBAI'
GO

--Step 3
Check .env file and all variable details

--Step 4
Install Python on server with Python 3.11.4

--Step 5
Run all packages which are listed in requirement.txt file
pip install -r "C:\requirements.txt"