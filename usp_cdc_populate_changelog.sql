USE <CDC DATABASE>
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[usp_cdc_populate_changelog]
	@destDB NVARCHAR(255), -- Database used for 'housing' all of the server's CDC objects e.g., <DBAUtility>..<tableName>_changelog 
	@destTable NVARCHAR(255), -- Table used within destination database e.g., <database>..<TASKDEFS>_changelog 
	@tableName NVARCHAR(255),  -- local CDC'd table name
	@tablePK NVARCHAR(255), -- PK(s) of the CDC'd table
	@to_date DATETIME = NULL
AS -- =========================================
-- CREATE STORED PROCEDURE
-- ! ~ ~ # # 
--  EXECUTE THE PROC WITHIN THE DATABASE OF THE TABLES BEING CAPTURED 
-- # # ~ ~ !
-- EXTENDED PROPERTIES:
--             PURPOSE: POPULATES <cdcTable>_changelog
--					HIGHDATE SHOULD BE PASSED TO @to_date AT RUNTIME TO ENSURE COMPLETENESS
--					HOWEVER, IF NO VALUE IS PASSED TO @to_date, GETDATE() AKA CURRENT DATE & TIME IS THEN USED

-- =========================================
    SET NOCOUNT OFF;

    BEGIN
----------------------------------------------------
-- VARIABLE DECLARATIONS
----------------------------------------------------
		-- Variables related to CDC functinality
		--	DO NOT ALTER
		DECLARE @sSQL NVARCHAR(MAX); -- used to build SQL statements
        DECLARE @from_lsn VARBINARY(10);
		DECLARE @from_lsn_nvar NVARCHAR(100); -- holds the converted BINARY to NVARCHAR string of @from_lsn
		DECLARE @from_lsn_out VARBINARY(10); -- holds the OUTPUT of the executesql related to the @from_lsn
        DECLARE @to_lsn VARBINARY(10);
		DECLARE @to_lsn_nvar NVARCHAR(100); -- holds the converted BINARY to NVARCHAR string of @to_lsn
        DECLARE @from_date DATETIME;
		DECLARE @from_date_out DATETIME; -- holds the OUTPUT of the executesql related to the @from_date
		DECLARE @min_lsn_date DATETIME;
		DECLARE @captured_instance NVARCHAR(255); -- 'capture_instance' of the CDC'd table
		DECLARE @cdcTable NVARCHAR(255); -- local CDC table for monitored table
		DECLARE @cdcTableGetAll NVARCHAR(255) -- local CDC function to get all changes of monitored table
		DECLARE @schemaTableName NVARCHAR(255) -- local CDC'd table table with schema
		DECLARE @sSQLPK NVARCHAR(MAX) = '';
		DECLARE @pkName NVARCHAR(255) = '';
		DECLARE @buildPK_convert NVARCHAR(MAX) = '';
		DECLARE @buildPK_convert_select NVARCHAR(MAX) = '';
		DECLARE @buildPK_create NVARCHAR(MAX) = '';
		DECLARE @buildPK_not_in NVARCHAR(MAX) = '';
		DECLARE @buildPK_orderby NVARCHAR(MAX) = '';
		DECLARE @buildPK_select NVARCHAR(MAX) = '';
		DECLARE @buildPK_upb NVARCHAR(MAX) = '';
		DECLARE @buildPK_up_join NVARCHAR(MAX) = '';
		DECLARE @sSQL1 NVARCHAR(MAX) = ''; -- used to build main SQL statement
		DECLARE @sSQLColsCASE NVARCHAR(MAX) = ''; -- used in the CASE statement to build a list all of the columns associated with the CDC'd table (excluding the table's PK)
		DECLARE @colName NVARCHAR(255) = ''; -- holds the 'column name' of each column within the CDC'd table
		DECLARE @buildCASE NVARCHAR(MAX) = ''; -- holds the CASE statement
		DECLARE @sSQL2 NVARCHAR(MAX) = ''; -- used to build main SQL statement
		DECLARE @sSQLColsIN NVARCHAR(MAX) = ''; -- used in the IN clause to build a list all of the columns associated with the CDC'd table (excluding the table's PK)
		DECLARE @buildIN NVARCHAR(MAX) = ''; -- holds the IN statement column names
		DECLARE @sSQL3 NVARCHAR(MAX) = ''; -- used to build main SQL statement
		DECLARE @sSQL4 NVARCHAR(MAX) = ''; -- used to build main SQL statement
		DECLARE @sSQL5 NVARCHAR(MAX) = ''; -- used to build main SQL statement
		DECLARE @execSQL NVARCHAR(MAX) = ''; -- concacts all of the above related SQL statement variables together
		DECLARE @error_message NVARCHAR(500);	
        DECLARE @rows_changed NVARCHAR(18);

----------------------------------------------------
-- START OF THE SCRIPT
----------------------------------------------------

		SELECT @destDB = RTRIM(LTRIM(@destDB))
		SELECT @destTable = RTRIM(LTRIM(@destTable))
		SELECT @tableName = RTRIM(LTRIM(@tableName))
		SELECT @tablePK = RTRIM(LTRIM(@tablePK))

		DECLARE @pk_list TABLE (id INT IDENTITY(1,1), pkName VARCHAR(128))

		INSERT INTO @pk_list (pkName) 
		SELECT item FROM DBAUtility.dbo.SplitStrings (@tablePK, ',')

		DECLARE cur_pk CURSOR FOR SELECT [pkName] FROM @pk_list
			OPEN cur_pk
				FETCH cur_pk INTO @pkName 
					WHILE @@FETCH_STATUS <> - 1
						BEGIN
							IF @@FETCH_STATUS <> - 2
							BEGIN
								SELECT @pkName = RTRIM(LTRIM(@pkName))
								SET @sSQLPK = '
					[' + LOWER(@pkName) + '] [INT] NULL,'
								SET @buildPK_create = @buildPK_create + @sSQLPK

								SET @sSQLPK = '
								  [' + @pkName + '_pk] ,'
								SET @buildPK_select = @buildPK_select + @sSQLPK
								
								SET @sSQLPK = '
										CONVERT(VARCHAR(128), [' + @pkName + '_pk]) AS [' + @pkName + '] ,'
								SET @buildPK_convert_select = @buildPK_convert_select + @sSQLPK

								SET @sSQLPK = '
										CONVERT(SQL_VARIANT, [' + @pkName + ']) AS [' + @pkName + '_pk] ,'
								SET @buildPK_convert = @buildPK_convert + @sSQLPK

								SET @sSQLPK = '
								[up_b].[' + @pkName + '_pk] ,
								'
								SET @buildPK_upb = @buildPK_upb + @sSQLPK

								SET @sSQLPK = '
								AND [up_a].[' + @pkName + '_pk] = [up_b].[' + @pkName + '_pk]
								'
								SET @buildPK_up_join = @buildPK_up_join + @sSQLPK

								SET @sSQLPK = '
								[tx].[' + @pkName + '_pk] ,
								'
								SET @buildPK_orderby = @buildPK_orderby + @sSQLPK

								SET @sSQLPK = ', ' + @pkName + ''
								SET @buildPK_not_in = @buildPK_not_in + @sSQLPK

							FETCH NEXT FROM cur_pk INTO @pkName
							END
						END
			CLOSE cur_pk
		DEALLOCATE cur_pk

		SELECT @buildPK_not_in = STUFF(@buildPK_not_in, 1, 2, '');

		-- create 'reporting' table if it doesn't exist
		SELECT @sSQL = N'
		USE [' + @destDB + ']

			IF NOT EXISTS (SELECT [name] FROM [sys].[tables] WHERE [name] = ''' + @destTable + ''')
				BEGIN 
					CREATE TABLE [' + @destDB + '].[dbo].[' + @destTable + '] (
					['+ LOWER(@destTable) + '_id] [BIGINT] IDENTITY(1,1) NOT NULL,
					[commit_time] [DATETIME] NULL,
					[table_name] [VARCHAR](255) NOT NULL, ' 
					+ @buildPK_create + '
					[column_name] [VARCHAR](128) NULL,
					[old_value] [VARCHAR](128) NULL,
					[new_value] [VARCHAR](128) NULL,
					[created_dt] [DATETIME] NOT NULL,
					[created_by] [VARCHAR](50) NOT NULL,
					[modified_dt] [DATETIME] NULL,
					[modified_by] [VARCHAR](50) NULL,
				
					CONSTRAINT [pk_' + LOWER(@destTable) + '] PRIMARY KEY CLUSTERED ([' + LOWER(@destTable) + '_id] ASC) 
					
					WITH (
						PAD_INDEX = OFF, 
						STATISTICS_NORECOMPUTE = OFF, 
						IGNORE_DUP_KEY = OFF, 
						ALLOW_ROW_LOCKS = ON, 
						ALLOW_PAGE_LOCKS = ON, 
						FILLFACTOR = 100
					) ON [PRIMARY]

				) ON [PRIMARY]
				;

				SET ANSI_PADDING OFF

				ALTER TABLE [dbo].[' + LOWER(@destTable) + '] ADD CONSTRAINT [df_' + LOWER(@destTable) + '_created_dt]  DEFAULT (GETDATE()) FOR [created_dt]
				ALTER TABLE [dbo].[' + LOWER(@destTable) + '] ADD CONSTRAINT [df_' + LOWER(@destTable) + '_created_by]  DEFAULT (SUSER_SNAME()) FOR [created_by]
			END
			;
			'
		EXEC (@sSQL)
		
		-- set @schemaTableName to schema.tableName
		SELECT @schemaTableName = 'dbo.' + @tableName + ''; 

		-- set @captured_instance to the corresponding instance based upon the 'tableName'
		SELECT @captured_instance = (SELECT capture_instance FROM cdc.change_tables WHERE source_object_id = (SELECT object_id FROM sys.tables WHERE name = @tableName))

		-- set @cdcTable to corresponding 'tableName'
		SELECT @cdcTable = (SELECT '[cdc].[' + name + ']' FROM sys.objects WHERE OBJECT_ID = (SELECT OBJECT_ID FROM cdc.change_tables WHERE capture_instance = @captured_instance))

		-- set @cdcTableGetAll to the corresponding 'tableName''s get_all_changes function
		SELECT @cdcTableGetAll = '[cdc].[fn_cdc_get_all_changes_dbo_' + @tableName + ']'; 

		-- set @to_date = parameter passed or GETDATE()
		SELECT  @to_date = COALESCE(@to_date, GETDATE())

		SELECT @sSQL = '
			SELECT @min_lsn_date = (SELECT MIN([sys].[fn_cdc_map_lsn_to_time]([__$start_lsn])) FROM ' + @cdcTable + ')
		'
		EXEC sp_executesql @sSQL, N'@min_lsn_date DATETIME OUTPUT', @min_lsn_date = @min_lsn_date OUTPUT
					
		-- if nothing is returned from the log table get the minimum LSN from the CDC table
		SELECT @sSQL = N'
            SELECT @from_date_out = COALESCE (
				(
					SELECT DATEADD(minute, 1, MAX([commit_time])) FROM [' + @destDB + '].[dbo].[' + @destTable + ']
                ) 
				, 
				( 
					SELECT MIN([sys].[fn_cdc_map_lsn_to_time]([__$start_lsn])) FROM ' + @cdcTable + '
				)
			); 
		'
		EXEC sp_executesql @sSQL, N'@from_date_out DATETIME OUTPUT', @from_date_out = @from_date OUTPUT
		--SELECT @from_date, @to_date
----------------------------------------------------
-- DATA INPUT VALIDATION AND ERROR HANDLING
----------------------------------------------------
	-- validate @to_date is not less than @from_date
        IF @to_date < @from_date
            BEGIN
                SET @error_message = '@to_date (' + CAST(@to_date AS VARCHAR(50)) + ') is less than @from_date (' + CAST(@from_date AS VARCHAR(50)) + ') - THIS WILL CAUSE THE CDC QUERY TO FAIL - HALTED!';
                GOTO ERROR;  
            END
		;
		
	-- validate @to_date is not prior to the @min_lsn_date (date of the first change record captured)
		IF @to_date < @min_lsn_date
			BEGIN
				SET @error_message = '@to_date (' + CAST(@to_date AS VARCHAR(50)) + ') is dated prior to the date of the first record captured by CDC ' + CAST(@min_lsn_date AS VARCHAR(50)) + ' - THIS WILL CAUSE THE CDC QUERY TO FAIL - HALTED!;'
				GOTO ERROR;
			END

----------------------------------------------------
-- MAIN SCRIPT
----------------------------------------------------
	-- get the LSN values for the timeframe (@to_date through @from_date)
		/*
		note: if the value for @from_lsn is PRIOR to the date and time CDC was enabled on the table (OR if @from_lsn or @to_lsn values are NULL) the following error will be returned on the origin date of CDC being enabled
			error: "an insufficient number of arguments were supplied for the procedure or function cdc.fn_cdc_get_all_changes_ ..."
			solution: roll the value forward incrementally until the error stop
		*/

        SELECT  @to_lsn = [sys].[fn_cdc_map_time_to_lsn]('largest less than', @to_date);

		SELECT @sSQL = N'
			SELECT  @from_lsn_out = CASE WHEN EXISTS ( 
				SELECT   1 FROM [' + @destDB + '].[dbo].[' + @destTable + '] 
			) 
			THEN 
				[sys].[fn_cdc_map_time_to_lsn](''smallest greater than'', ''' + CAST(@from_date AS NVARCHAR(50)) + ''')
			ELSE 
				[sys].[fn_cdc_map_time_to_lsn](''smallest greater than or equal'', ''' + CAST(@from_date AS NVARCHAR(50)) + ''')
			END;
		'		
		EXEC sp_executesql @sSQL, N'@from_lsn_out VARBINARY(10) OUTPUT', @from_lsn_out = @from_lsn OUTPUT

		-- convert binary parameters to nvarchar for use in dynamic sql
		SELECT @to_lsn_nvar = [master].[dbo].[fn_varbintohexstr](@to_lsn);
		SELECT @from_lsn_nvar = [master].[dbo].[fn_varbintohexstr](@from_lsn);

		-- BUILD SQL STATEMENT
		BEGIN TRY
			SET @sSQL1 = '
						INSERT  INTO [' + @destDB + '].[dbo].[' + @destTable + ']
								( [commit_time] ,
								  [table_name] ,
								  --[' + @tablePK + '] ,'
								  + REPLACE(@buildPK_select, '_pk', '') + '
								  [column_name] ,
								  [old_value] ,
								  [new_value] 
								)
								SELECT  CONVERT(DATETIME, [commit_time]) AS [commit_time] ,
										CONVERT(VARCHAR(128), [table_name]) AS [table_name] ,
										--CONVERT(VARCHAR(128), [' + @tablePK + ']) AS [' + @tablePK + '] ,'
										+ @buildPK_convert_select + '
										CONVERT(VARCHAR(128), [column_name]) AS [column_name] ,
										CONVERT(VARCHAR(128), [old_value]) AS [old_value] ,
										CONVERT(VARCHAR(128), [new_value]) AS [new_value] 
								FROM    ( SELECT    [sys].[fn_cdc_map_lsn_to_time]([up_b].[__$start_lsn]) AS [commit_time] ,
													''' + @schemaTableName + ''' AS [table_name] ,
													--[up_b].[' + @tablePK + '] , '
													+ @buildPK_upb + '
													[up_b].[column_name] ,
													[up_b].[old_value] ,
													[up_a].[new_value]
										  FROM      ( SELECT    [__$start_lsn] ,
																[column_name] ,
																[old_value] ,
																--[' + @tablePK + '] '
																+ STUFF(@buildPK_select, LEN(@buildPK_select), 1, '') + '
													  FROM      ( SELECT    [__$start_lsn] ,
																			--CONVERT(SQL_VARIANT, [' + @tablePK + ']) AS [' + @tablePK + '] , '
																			+ @buildPK_convert + '
			'

			DECLARE curCols CURSOR FOR SELECT name FROM sys.columns WHERE object_id = (SELECT object_id FROM sys.tables WHERE name = @tableName)
				OPEN curCols
					FETCH curCols INTO @colName
						WHILE @@FETCH_STATUS <> - 1
						BEGIN
							IF @@FETCH_STATUS <> - 2
							BEGIN
								SET @sSQLColsCASE = ',
									CASE WHEN (
										[sys].[fn_cdc_is_bit_set] (
											[sys].[fn_cdc_get_column_ordinal](''' + @captured_instance + ''', ''' + @colName + '''), [__$update_mask]
											) = 1 
										) THEN CONVERT(SQL_VARIANT, [' + @colName + '])
										ELSE NULL
										END AS [' + @colName + ']							
								'
								SET @buildCASE = @buildCASE + @sSQLColsCASE
								FETCH NEXT FROM curCols INTO @colName
							END
						END
						SET @buildCASE = STUFF(@buildCASE, 1, 1, '');
				CLOSE curCols	
								
			SET @sSQL2 = '				
                FROM ' + @cdcTableGetAll + ' (' + @from_lsn_nvar + ', ' + @to_lsn_nvar + ', ''all update old'')
                WHERE     [__$operation] = 3
				) AS t1 UNPIVOT ( [old_value] FOR [column_name] IN ( '

				OPEN curCols
					FETCH curCols INTO @colName
						WHILE @@FETCH_STATUS <> - 1
						BEGIN
							IF @@FETCH_STATUS <> - 2
							BEGIN
								SET @sSQLColsIN = ', [' + @colName + ']'
								SET @buildIN = @buildIN + @sSQLColsIN				
								FETCH NEXT FROM curCols INTO @colName
							END
						END
						SET @buildIN = STUFF(@buildIN, 1, 1, '')
				CLOSE curCols
			DEALLOCATE curCols

			SET @sSQL3 = ') ) AS unp
                        ) AS [up_b] -- BEFORE UPDATE
                        INNER JOIN ( SELECT [__$start_lsn] ,
                                            [column_name] ,
                                            [new_value] ,
                                            --[' + @tablePK + '] '
											+ STUFF(@buildPK_select, LEN(@buildPK_select), 1, '') + '
                                        FROM   ( SELECT    [__$start_lsn] ,
                                                        --CONVERT(SQL_VARIANT, [' + @tablePK + ']) AS [' + @tablePK + '] , '
														+ @buildPK_convert + '
						'
			--@buildCASE here
                                                                       
			SET @sSQL4 = '
			FROM ' + @cdcTableGetAll + ' (' + @from_lsn_nvar + ', ' + @to_lsn_nvar + ', ''all update old'') -- ''all update old'' is not necessary here
																  WHERE     [__$operation] = 4
																) AS t2 UNPIVOT ( [new_value] FOR [column_name] IN (
			' 
			--@buildIN here

			SET @sSQL5 = '
			) ) AS unp
				) AS [up_a] -- AFTER UPDATE
			ON [up_b].[__$start_lsn] = [up_a].[__$start_lsn]
				AND [up_b].[column_name] = [up_a].[column_name]
--				AND [up_a].[' + @tablePK + '] = [up_b].[' + @tablePK + '] ' 
				+ @buildPK_up_join + '
			UNION ALL
			SELECT    [sys].[fn_cdc_map_lsn_to_time]([__$start_lsn]) AS [commit_time] ,
				''' + @schemaTableName + ''' AS [table_name] ,
				--CONVERT(SQL_VARIANT, [' + @tablePK + ']) AS [' + @tablePK + '] , '
				+ @buildPK_convert + '
				NULL AS [column_name] ,
				''DELETED RECORD'' AS [old_value] ,
				NULL AS [new_value]
			FROM ' + @cdcTableGetAll + ' (' + @from_lsn_nvar + ', ' + @to_lsn_nvar + ', ''all'')
			WHERE    
				 [__$operation] = 1 --DELETE
			UNION ALL
			SELECT    [sys].[fn_cdc_map_lsn_to_time]([__$start_lsn]) AS [commit_time] ,
				''' + @schemaTableName + ''' AS [table_name] ,
				--CONVERT(SQL_VARIANT, [' + @tablePK + ']) AS [' + @tablePK + '] , ' 
				+ @buildPK_convert + '
				NULL AS [column_name] ,
				NULL AS [old_value] ,
				''NEW RECORD'' AS [new_value]
			FROM ' + @cdcTableGetAll + ' (' + @from_lsn_nvar + ', ' + @to_lsn_nvar + ', ''all'')
			WHERE     
				[__$operation] = 2 --INSERT
			) tx
			--WHERE
				--[tx].[commit_time] NOT IN (SELECT MAX(commit_time) FROM [' + @destDB + '].[dbo].[' + @destTable +'])
			ORDER BY 
				[tx].[commit_time] ,
				--[tx].[' + @tablePK + '] , '
				+ @buildPK_orderby + '
				[tx].[column_name]
			; 
			'           
			EXEC (@sSQL1 + @buildCASE + @sSQL2 + @buildIN + @sSQL3 + @buildCASE + @sSQL4 + @buildIN + @sSQL5)
			
			SET @rows_changed = @@ROWCOUNT;

        END TRY

        BEGIN CATCH

            SET @error_message = 'No ' + @tableName + ' changes were detected! No updates were made to the ' + @destTable + '.';
				PRINT @error_message;
            GOTO SUCCESS;

        END CATCH
		;      
		 
        SET @error_message = @rows_changed + ' ' + @tableName + ' changes were inserted to the ' + @destTable + '.';
			PRINT @error_message;  
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
	;
GO


