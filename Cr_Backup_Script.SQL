--To export using bcp utility
/*
The following example exports data using Azure AD Username and Password where user and password is an AAD credential.
The example exports table bcptest from database testdb from Azure server aadserver.database.windows.net and stores the data in file c:\last\data1.dat:
bcp bcptest out "c:\last\data1.dat" -c -t -S aadserver.database.windows.net -d testdb -G -U alice@aadtest.onmicrosoft.com -P xxxxx

bcp TE_DM_Policy out "c:\temp\TE_DM_Policy.dat" -c -t -S rlg-test-cdp-uks-sqlsrvr.database.windows.net -d rlg-test-cdp-uks-sqldb02 -G -U Ramasamy.Baskar@royallondon.com -P MYRL09!arb1610s76a1


The following example imports data using Azure AD Username and Password where user and password is an AAD credential. 
The example imports data from file c:\last\data1.dat into table bcptest for database testdb on Azure server aadserver.database.windows.net using Azure AD User/Password:
bcp bcptest in "c:\last\data1.dat" -c -t -S aadserver.database.windows.net -d testdb -G -U alice@aadtest.onmicrosoft.com -P xxxxx
*/
--Script to create Back up script using bcp utility 

select 'bcp [rlg-test-cdp-uks-sqldb02].[qa].' + name + ' out c:\temp\' + name + '.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!'
from sys.tables where SCHEMA_NAME(schema_id) = 'qa'

bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_DM_Individual out c:\temp\TE_DM_Individual.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_DM_Party_Payment_Detail out c:\temp\TE_DM_Party_Payment_Detail.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_DM_Employment out c:\temp\TE_DM_Employment.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_DM_Account out c:\temp\TE_DM_Account.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_DM_Policy_Cover out c:\temp\TE_DM_Policy_Cover.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_DM_Adviser out c:\temp\TE_DM_Adviser.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_DM_Email out c:\temp\TE_DM_Email.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_DM_Party_Phone out c:\temp\TE_DM_Party_Phone.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TMP_EXTLocation_D out c:\temp\TMP_EXTLocation_D.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_EXTLocation_Detail out c:\temp\TE_EXTLocation_Detail.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TMP_EXTLocation_S out c:\temp\TMP_EXTLocation_S.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_DM_Policy out c:\temp\TE_DM_Policy.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_EXTLocation_Summary out c:\temp\TE_EXTLocation_Summary.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_DM_Policy_Details out c:\temp\TE_DM_Policy_Details.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_DM_Party_Policy_Role out c:\temp\TE_DM_Party_Policy_Role.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_DM_Location out c:\temp\TE_DM_Location.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_DM_Party out c:\temp\TE_DM_Party.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_DM_Party_Location out c:\temp\TE_DM_Party_Location.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!
bcp [rlg-test-cdp-uks-sqldb02].[qa].TE_DM_Policy_Clause out c:\temp\TE_DM_Policy_Clause.csv -c -t, -S tcp:rlg-test-cdp-uks-sqlsrvr.database.windows.net -U qa_user -P QABackup2021!