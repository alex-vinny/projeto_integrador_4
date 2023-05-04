IF NOT EXISTS(
	SELECT
		*
	FROM
		information_schema.tables
	WHERE
		table_schema = 'dbo'
		AND table_name = 'stocks')
BEGIN
	CREATE TABLE dbo.stocks
	(
		id BIGINT IDENTITY(1,1) NOT NULL,
		code VARCHAR(20) NOT NULL,
		description VARCHAR(200),
		last_integration DATETIME,
		CONSTRAINT pk_stocks PRIMARY KEY(id),
		CONSTRAINT uk_stocks UNIQUE (code) 
	)
END

IF NOT EXISTS(
	SELECT
		*
	FROM
		information_schema.tables
	WHERE
		table_schema = 'dbo'
		AND table_name = 'stock_data')
BEGIN
	CREATE TABLE dbo.stock_data
	(
		id BIGINT IDENTITY(1,1) NOT NULL,
		stock_id BIGINT NOT NULL,
		stock_date DATETIME NOT NULL,
		open_value FLOAT,
		high_value FLOAT,
		low_value FLOAT,
		close_value FLOAT,		
		adjclose_value FLOAT,
		volume_value FLOAT,
		CONSTRAINT pk_stock_data PRIMARY KEY(id),
		CONSTRAINT fk_stocks_id FOREIGN KEY (stock_id) REFERENCES dbo.stocks(id)
	)
END

IF NOT EXISTS(
	SELECT
		*
	FROM
		information_schema.tables
	WHERE
		table_schema = 'dbo'
		AND table_name = 'stock_calculation')
BEGIN
	CREATE TABLE dbo.stock_calculation
	(
		id BIGINT IDENTITY(1,1) NOT NULL,
		stock_data_id BIGINT NOT NULL,
		stock_date DATETIME NOT NULL,
		process_date DATETIME,
		results FLOAT,
		positive FLOAT,
		negative FLOAT,
		positive_mean FLOAT,
		negative_mean FLOAT,
		rsi FLOAT,
		opportunity BIT,
		to_buy BIT,
		to_sell BIT,		
		CONSTRAINT pk_stock_calculation PRIMARY KEY(id),
		CONSTRAINT fk_stock_data_id FOREIGN KEY (stock_data_id) REFERENCES dbo.stock_data(id)
	)
END

IF EXISTS (SELECT * 
	FROM   sys.objects
	WHERE  id = object_id(N'[dbo].[usp_get_stockdata]')
	AND OBJECTPROPERTY(id, N'IsProcedure') = 1 )
BEGIN
    DROP PROCEDURE [dbo].[usp_get_stockdata];
END

CREATE PROCEDURE [dbo].[usp_get_stockdata]
(
	@stock_id BIGINT,
	@initDate DATETIME,
	@endDate DATETIME
)
AS
BEGIN
	SELECT * FROM
	(
		SELECT
			*
		FROM dbo.stock_data
		WHERE stock_id = @stock_id
			AND stock_date BETWEEN @initDate AND @endDate
		--ORDER BY stock_date
		UNION ALL
		SELECT 
			TOP(1) *
		FROM dbo.stock_data
		WHERE stock_id = @stock_id
			AND stock_date < @initDate
		ORDER BY stock_date DESC
	) AS tmp
	ORDER BY tmp.stock_date
END

CREATE NONCLUSTERED INDEX [IX_stock_data_stock_date] ON [dbo].[stock_data] ([stock_id],[stock_date])
INCLUDE ([open_value],[high_value],[low_value],[close_value],[adjclose_value],[volume_value])

ALTER TABLE stock_calculation ADD UNIQUE ([stock_data_id]);
