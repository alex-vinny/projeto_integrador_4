IF NOT EXISTS(SELECT 1 FROM dbo.stocks)
BEGIN
	INSERT INTO dbo.stocks VALUES ('MGLU3.SA', 'Magazine Luiza S.A.', '2000-01-01');
	INSERT INTO dbo.stocks VALUES ('VIIA3.SA', 'Via S.A.', '2000-01-01');
	INSERT INTO dbo.stocks VALUES ('AMER3.SA', 'Americanas S.A.', '2000-01-01');
	INSERT INTO dbo.stocks VALUES ('PETR4.SA', 'Petróleo Brasileiro S.A.', '2000-01-01');
	INSERT INTO dbo.stocks VALUES ('ABEV3.SA', 'Ambev S.A.', '2000-01-01');
	INSERT INTO dbo.stocks VALUES ('VALE3.SA', 'Vale S.A.', '2000-01-01');
	INSERT INTO dbo.stocks VALUES ('HAPV3.SA', 'Hapvida Participações e Investimentos S.A.', '2000-01-01');	
END
