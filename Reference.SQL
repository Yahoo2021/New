select * 
  from dw.source_system

select * 
  from dw.Scheme

select * 
  from dw.party_type

select * 
  from dw.Marital_Status

SELECT DISTINCT Party_Type_Id 
  FROM dw.Party

--Party
select * 
  from dw.Party
 where Source_Reference = 49124941--10005355 

select * 
  from Ext.ExtParty
 where SourceUnique_id = 8828434

select * 
  from Stage.StageCustomer
 where Client_reference = 49124941

select * 
  from dw.policy_status

select Policy_Reference , Count(*) as Total
  from dw.Policy 
 group by Policy_Reference
having count(*) > 1 

select * 
  from Dw.Policy
 where Policy_Reference = '5369719'
 order by Policy_id

select * 
  from Ext.extPolicy

select * 
  from Stage.StagePolicy

SELECT isDate('2021-13-11')

select * from [tSQLt].[Tests]

select * from [tSQLt].[TestClasses]

select * from [tSQLt].[Private_SysIndexes]



select * from [Dw].[vwEmail]
