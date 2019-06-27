USE <¿?
GO
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_cdc_query_data] 
	@table AS VARCHAR(255),
	@from_date AS DATETIME,
	@to_date AS DATETIME = NULL
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	DECLARE @sSQL AS NVARCHAR(MAX);
	DECLARE @error_message AS VARCHAR(MAX);
	DECLARE @max_created_dt DATETIME;

	SELECT @table = RTRIM(LTRIM(@table)) -- clean @table
	SELECT @to_date = COALESCE(@to_date, GETDATE()) -- set @to_date = today if null

	IF @from_date >= @to_date
		BEGIN
			SET @error_message = '@from_date (' + CONVERT(VARCHAR(10), @from_date, 101) + ') is >= @to_date (' + CONVERT(VARCHAR(10), @to_date, 101) +').'
			GOTO ERROR;
		END

	-- if changelog doesn't doesn't exist for @table then throw error, else set @table = CDC'd table
	IF NOT EXISTS (SELECT [name] FROM [sys].[tables] WHERE [name] LIKE '' + @table + '%_ChangeLog')
		BEGIN
			SET @error_message = '@table (' + CAST(@table AS VARCHAR(255)) + ') does not exist. Please check the table is being captured !';
            GOTO ERROR;  
		END
	SELECT @table = (SELECT [name] FROM [sys].[tables] WHERE [name] LIKE '' + @table + '%_ChangeLog')

	SELECT @sSQL = '
		SELECT @max_created_dt = (SELECT MAX(CONVERT(NVARCHAR(10), created_dt, 101))	FROM ' + @table + ')
	'
	EXEC sp_executesql @sSQL, N'@max_created_dt NVARCHAR(10) OUTPUT', @max_created_dt = @max_created_dt OUTPUT 
	
	SELECT @sSQL = 'ATTENTION: Data only accurate up until >>    ' + CONVERT(NVARCHAR(10), @max_created_dt, 101) + '   << ! IF this date is inaccurate please execute the respective usp_cdc_populate_changelog stored procedure and re-run usp_cdc_query_data after its completion.' 
	SELECT @sSQL AS 'ATTN: Data is only accurate as of the date BELOW'


	SELECT @sSQL = '
		SELECT * 
		FROM ' + @table + '
		WHERE
			commit_time >= ''' + CONVERT(VARCHAR(10), @from_date, 101) + ''' AND
			commit_time <= ''' + CONVERT(VARCHAR(10), @to_date, 101) + '''

	'
	EXEC (@sSQL)
	GOTO SUCCESS;

	ERROR:
	RAISERROR (
		@error_message,  
		16, -- severity
		1 -- state
	)
	;

	SUCCESS:
	
END

GO


