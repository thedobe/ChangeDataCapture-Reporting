# ChangeDataCapture-Reporting

change data capture being a PITA to implement? One sproc to rule them all.

# usp_cdc_populate_changelog
Will populate a 'reporting' table 

params: 
  @destDB NVARCHAR(255), -- Database used for 'housing' all of the server's CDC objects 
  @destTable NVARCHAR(255), -- Table used within destination database
  @tableName NVARCHAR(255),  -- local CDC'd table name
  @tablePK NVARCHAR(255), -- PK(s) of the CDC'd table e.g., 'myPK' or 'myPK, anotherPK, oneMorePK'
  @to_date DATETIME = NULL

# usp_cdc_query_data
Will spit out data based upon the above created (or existing) table

