---Plan Sync Demo

--Source data for PlanSync
SELECT *
  FROM [alisdevb].[dbo].[ADF_PlanSync_Output]

--PanSync Count of records
SELECT COUNT(*) as Total_Records
  FROM [alisdevb].[dbo].[ADF_PlanSync_Output] --33933 

--To Check duplicte records
SELECT PlanId, Count(*) 
  FROM [alisdevb].[dbo].[ADF_PlanSync_Output] 
 GROUP By PlanId 
 HAVING Count(*) > 1 -- 0 records

--CDP Verification

--Staging Schema
SELECT * 
  FROM Stage.StagePolicy 
 
SELECT COUNT(*) as TotalRecords_Staging
  FROM Stage.StagePolicy 
  
--Dw Target Schema   
SELECT COUNT(*) as TotalRecords_Target
  FROM Dw.Policy 

SELECT * 
  FROM Dw.Policy_Details

  