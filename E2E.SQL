--E2E


SELECT * 
  FROM (

SELECT P.Policy_Id,P.Policy_Reference,PY.Source_Reference as "Source_Reference/ALISID",PD.Policy_Status_Id, PD.Product_Id
      ,PPR.Party_Id,PPR.Role_Type_Id as "Role_Type_ID(CDP)",RTM.Role_Type_Source_Id as "RoleId(ALIS)"
	  ,PY.Party_Type_Id
	  ,PC.Cover_Type_Id, PC.Cover_Reference,CT.Cover_Type,PC.Initial_Sum_Assured,PC.Current_Sum_Assured
	  ,Adviser_Name
	  ,I.Source_Reference,I.Title,ForeName,I.Middle_Name,I.Surname,I.Sex, I.Date_Of_Birth,I.Marital_Status_Id,MS.Marital_Status
      ,OCS_Received,OCS_Received_Date,Incapacity_Definition,Percentage_Split
	  ,A.Start_Date as Adviser_Start_Date,A.End_Date as Adviser_End_Date
	  ,I.Start_Date as Individual_Start_Date,I.End_Date as Individual_End_Date
	  ,E.Email_Type_Id, Email_Address
	  ,PH.Phone_Number,PH.Start_Date as PH_Start_Date,PH.End_Date as PH_End_Date
	  ,PC.Start_Date as PolicyCover_Start_Date, PC.End_Date as Policy_Cover_End_date
  FROM Dw.Policy P
  Left Join Dw.Policy_Details PD        on PD.Policy_Id = P.Policy_Id --and PD.End_Date > GetDate()
  Left Join Dw.Party_Policy_Role PPR    on P.Policy_Id = PPR.Policy_Id
  Left Join Dw.Party PY                 on PPR.Party_Id = PY.Party_Id 
  Left Join Dw.Adviser A                on A.Party_Id = PPR.Party_Id --and A.End_Date > GetDate()
  Left Join Dw.Individual I             on I.Party_Id = PPR.Party_Id --and I.End_Date > GetDate()
  Left Join Dw.Marital_Status MS        on MS.Marital_Status_Id = I.Marital_Status_Id
  Left Join Reference.Role_Type_Map RTM on RTM.Role_Type_Id = PPR.Role_Type_Id
  Left Join Dw.Policy_Cover PC          on PC.Policy_Id = P.Policy_Id
  Left Join Dw.Cover_Type CT            on CT.Cover_Type_Id = PC.Cover_Type_Id
  Left Join Dw.Email E                  on E.Party_Id = PY.Party_Id
  Left Join Dw.Party_Phone PH           on PH.Party_Id = PY.Party_Id

  ) X  
  --WHERE Policy_Reference = 79764222
  --WHERE Policy_Reference = 79778225
WHERE Phone_Number is NOT NULL

 ORDER By Policy_Reference, Party_Id,"Role_Type_ID(CDP)" ,Adviser_Start_Date, Adviser_End_Date


--SELECT * FROM Dw.Policy_Details