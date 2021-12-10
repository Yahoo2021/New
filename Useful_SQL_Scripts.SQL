DECLARE @sql AS nvarchar(MAX)
declare @Month Nvarchar(1000)

--DROP TABLE #temp
select distinct YYYYMM into #temp from #AccPurch AS ap
SELECT  @Month = COALESCE(@Month, '') + '[' + CAST(YYYYMM AS VarChar(8)) + '],' FROM    #temp

SELECT   @Month= LEFT(@Month,len(@Month)-1)


SET @sql = N'SELECT UserID, '+ @Month + N' into ##final_Donovan_12345 FROM (
Select ap.AccPurch ,
       ap.YYYYMM ,
       ap.UserID ,
       ap.AccountNumber
FROM #AccPurch AS ap 
) p
Pivot (SUM(AccPurch) FOR YYYYMM IN ('+@Month+ N')) as pvt'


EXEC sp_executesql @sql

Select * INTO #final From ##final_Donovan_12345

DROP TABLE  ##final_Donovan_12345

Select * From #final AS f