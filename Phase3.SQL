WITH 
CTE_Individual as ( SELECT * FROM Dw.Individual WHERE End_Date > GetDate() )

SELECT IND.Individual_Id, IND.Party_Id
      ,(SELECT Count(*) FROM Dw.Party_Phone       WHERE END_Date > GetDate() and Party_Id = IND.Party_Id) as Phone_COUNT
	  ,(SELECT Count(*) FROM Dw.Email             WHERE END_Date > GetDate() and Party_Id = IND.Party_Id) as Email_COUNT
	  ,(SELECT Count(*) FROM Dw.Employment        WHERE END_Date > GetDate() and Party_Id = IND.Party_Id AND (Occupation_Id > 0 or Employment_Type_Id > 0)) as Employment_COUNT
      ,(SELECT Count(*) FROM Dw.Party_Location PL WHERE END_Date > GetDate() and Party_Id = IND.Party_Id) as PartyLocation_COUNT
	  ,(SELECT Count(*) FROM Dw.Party_Payment_Detail     WHERE END_Date > GetDate() and Party_Id = IND.Party_Id) as PartyPayment_Count
	  ,(SELECT Count(*) FROM Dw.Party_Policy_Role WHERE END_Date > GetDate() and Party_Id = IND.Party_Id) as RoleCount
  FROM CTE_Individual IND


  --SELECT * FROM Qa.JSONTable
--SELECT * FROM Dw.Party_Payment_Detail

--SELECT Count(*) FROM Dw.Party_Policy_Role

select * from dw.individual where party_id = 46613

select * from dw.Policy where Policy_Reference in ( 82934838, 82935839,82940610)


SELECT * FROM QA.Jsontable
 WHERE ObjectType = 'EventHub_PolicyCover'

 SELECT * FROM Dw.Policy_Cover WHERE Policy_Id = 23512
 SELECT * FROM Dw.Policy_Clause where policy_cover_id = 29858
 SELECT * FROM Dw.Policy WHERE Policy_Id = 23520