/*
SELECT Name, COUNT(1) 
  FROM (
        SELECT M.TopicId,T.Name,M.PrimaryKey,M.SequenceNo,M.BlobPath,M.MessageType,M.Created,M.LastModified
          FROM Ingress.Message M
          Join Ingress.Topic   T
            on M.TopicId = T.Id
		) T
 --WHERE MessageType = 'CoverSync'
WITH (NOLOCK) GROUP BY Name


SELECT TopicId, COUNT(1) FROM Ingress.Message WITH (NOLOCK) GROUP BY TopicId


SELECT TopicId, MessageType,Name, COUNT(1) as TotalMessages
  FROM  (
        SELECT M.TopicId,T.Name,M.PrimaryKey,M.SequenceNo,M.BlobPath,M.MessageType,M.Created,M.LastModified
          FROM Ingress.Message M WITH (NOLOCK)
          Join Ingress.Topic   T WITH (NOLOCK)
            on M.TopicId = T.Id
		) T   GROUP BY TopicId,MessageType,Name
 ORDER By TopicId


SELECT COUNT(1) as Total FROM [Ingress].[Checkpointing] WITH (NOLOCK) --30762

SELECT t.Name, COUNT(1)
FROM Ingress.Message m WITH (NOLOCK) INNER JOIN Ingress.Topic t  WITH (NOLOCK)on m.TopicId = t.Id GROUP BY t.Name

SELECT COUNT(1) FROM [Dw].[Policy] with (NOLOCK) --1167171

*/
SELECT @@ServerName as ServerName,TopicId, MessageType,Name, BlobPath,Created,COUNT(1) as TotalMessages
  FROM  (
        SELECT M.TopicId,T.Name,M.PrimaryKey,M.SequenceNo,M.BlobPath,M.MessageType,M.Created,M.LastModified
          FROM Ingress.Message M WITH (NOLOCK)
          Join Ingress.Topic   T WITH (NOLOCK)
            on M.TopicId = T.Id
		) T   
 WHERE CAST(Created as Date) >= '2021-12-01'--CAST(GetDate() as Date)
 GROUP BY TopicId,MessageType,Name,Created,BlobPath
 ORDER By TopicId,Created DESC

 SELECT * FROM Ingress.Message 
  WHERE CAST(Created as Date) >= '2021-09-30'--CAST(GetDate() as Date)
   ORDER By Created DESC

SELECT @@ServerName as ServerName,MessageType,CAST(Created as DATE) as DT,COUNT(1) as Total
  FROM Ingress.Message 
 WHERE CAST(Created as Date) >= '2021-09-30'--CAST(GetDate() as Date)
 GROUP By MessageType,CAST(Created as DATE)
 ORDER By CAST(Created as DATE) DESC,MessageType