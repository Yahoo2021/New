
DECLARE @ObjectIds AS Dw.udt_Object_Id
INSERT INTO @ObjectIds
VALUES
(46600)
exec Dw.usp_Account_JSON @PartyIds=@ObjectIds