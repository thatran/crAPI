# Databricks notebook source
# Created by PG/DG - 01/31/2024

# COMMAND ----------

# NoteBook Start Time in EST
import os.path
import datetime
import time
from datetime import datetime, timedelta
import pytz

# Get the current date and time in UTC
UTC_Start = datetime.now(pytz.utc)

# Convert UTC time to Eastern Standard Time (EST)
est = pytz.timezone('US/Eastern')
EST_Start = UTC_Start.astimezone(est)

# Get the current date and time in EST with nanoseconds
Notebook_Start = EST_Start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

print(Notebook_Start)

# COMMAND ----------

# MAGIC %run ./../IdeationConfig

# COMMAND ----------

# MAGIC %run ./vw_Common

# COMMAND ----------

# MAGIC %run ./Vendor_Extracts_Code

# COMMAND ----------

#NoteBook Logic Start
import uuid

dbutils.widgets.text("client_code",'')
dbutils.widgets.text("database_name",'')
dbutils.widgets.text("database_name_prc",'')
dbutils.widgets.text("lob_code",'')
dbutils.widgets.text("host_system", "")
dbutils.widgets.text("monthly_weekly", "")

client_code      = dbutils.widgets.get("client_code")
database_name    = dbutils.widgets.get("database_name")
database_name_prc    = dbutils.widgets.get("database_name_prc")
lob_code         = dbutils.widgets.get("lob_code")
host_system      = dbutils.widgets.get("host_system")
monthly_weekly      = dbutils.widgets.get("monthly_weekly")

# static parameters
file_extension   = '.txt'
file_name = '' # will be assigbned for each file type
vendor = 'vw'
quote_option = 'false'

# COMMAND ----------

lake_container = "ideation"
client_folder_name = client_code.lower()
lob_folder_name = lob_code.lower()
host_folder_name = host_system.lower()
vendor_name = vendor.lower()
lake_destination_full_path = f'abfss://{lake_container}@{account_name}/{client_folder_name}/{lob_folder_name}/{host_folder_name}/{vendor_name}/'

# COMMAND ----------

#1 HBCDM File weekly 
# FileName: RoperStFrancis_vw_HBCDM_YYYYMMDD.txt  

if monthly_weekly == 'weekly':

    Notebook_Start_Demo = get_current_timestamp_est()
    print(Notebook_Start_Demo)

    # DimAudit_Start:
    DimAudit_Start(client_code, vendor, Notebook_Start_Demo,None ,None ,None)

    #DimAudit_Start
    import pyodbc

    # Define your connection parameters
    server = '10.20.30.40'
    database = 'VI_Stuff'
    username = 'WQELPRES_'
    password = '$g7Xv!QzR9m@L2e#T8bW^uYpK4s&JhNcVxAoZqMd'
    conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Connect to the SQL Server database
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()

    # Get parameters from Databricks notebook
    start_Time = Notebook_Start_Demo
    #source_file_path = 'NULL'  
    table_name = client_code + '_' + vendor 
    PkgName = notebook_name
    DimAudit = 'dbo.' + vendor + '_' + 'DimAudit'

    # Define and execute your SQL query using Pyodbc syntax
    query = '''
    DECLARE @curDate DATETIME
    DECLARE @TableName VARCHAR(MAX)
    DECLARE @PkgName VARCHAR(MAX)

    SET @curDate = ?
    SET @TableName = ?
    SET @PkgName = ?

    INSERT INTO ''' + DimAudit + '''
               (ParentAuditKey
               ,[TableName]
               ,[PkgName]       
               ,[ExecStartDT]
               ,[ExecStopDT]           
               ,[ExtractRowCnt]
               ,[InsertRowCnt]   
               ,[UpdateRowCnt]                  
               ,[ErrorRowCnt]
               ,[TableInitialRowCnt]
               ,[TableFinalRowCnt]
               ,[TableMaxDateTime]
               ,[SuccessfulProcessingInd]
               ,[SourceFilePath])
         VALUES
               (1
               ,@TableName
               ,@PkgName       
               ,@curDate
               ,NULL           
               ,NULL
               ,0
               ,0          
               ,NULL
               ,0
               ,0
               ,NULL
               ,'N'
               ,NULL)
    '''
    cursor.execute(query,start_Time, table_name,PkgName)

    # Commit the transaction
    conn.commit()

    # Fetch the results of the SELECT statement
    Max_AuditKey = "SELECT max(Auditkey) FROM " + DimAudit + " WHERE [ExecStartDT] = ? and [TableName] = ?"
    select_cursor = conn.cursor()
    select_cursor.execute(Max_AuditKey, start_Time,  table_name)
    results = select_cursor.fetchone()

    # Print the fetched results
    for row in results:
        AuditKey = row
    print(AuditKey)

    file_name = client_code +'_' + vendor.capitalize() + '_' + 'HBCDM'
    df = Get_vw_HBCDM(database_name)
    row_count = df.count()
    RunIdeationLocal_txt(RunDate,'|', file_extension, quote_option)

    # DimAudit_Update:
    output_file_path = lake_destination_full_path + file_name + output_file_name
    Notebook_Stop_Demo = get_current_timestamp_est()
    DimAudit_Update(Notebook_Start_Demo,Notebook_Stop_Demo,row_count, output_file_path)


    #DimAudit_End
    import pyodbc

    # Get the current date and time in UTC
    UTC_End = datetime.now(pytz.utc)

    # Convert UTC time to Eastern Standard Time (EST)
    est = pytz.timezone('US/Eastern')
    EST_End = UTC_End.astimezone(est)

    # Get the current date and time in EST with nanoseconds
    Notebook_End = EST_End.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    print(Notebook_End)

    # Define your connection parameters
    server = '10.20.30.40'
    database = 'VI_Stuff'
    username = 'WQELPRES_'
    password = '$g7Xv!QzR9m@L2e#T8bW^uYpK4s&JhNcVxAoZqMd'
    conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'


    Parent_AuditKey = AuditKey
    #print(Parent_AuditKey)
    ExtractRowCount_DimAudit = row_count

    # Define your SQL query separately
    query = '''
        UPDATE ''' + DimAudit + '''
        SET [ExecStopDT] = ?,
            ExtractRowCnt = ?,
            SuccessfulProcessingInd = 'Y',
            TableFinalRowCnt = ?,
            SourceFilePath = ?,
            ParentAuditKey = ?
        WHERE AuditKey = ? AND [TableName] = ? AND [ExecStartDT] = ?
    '''

    #exit()
    # Execute the query
    cursor.execute(query, (Notebook_End, ExtractRowCount_DimAudit, ExtractRowCount_DimAudit,output_file_path,Parent_AuditKey, Parent_AuditKey, table_name,start_Time))

    # Commit the transaction
    conn.commit()

    # Close the connection
    conn.close()


# COMMAND ----------

#2 PBCDM File  , Frequency - weekly
# FileName: RoperStFrancis_vw_PBCDM_YYYYMMDD.txt  

if monthly_weekly.lower() == 'weekly':

    Notebook_Start_Demo = get_current_timestamp_est()
    print(Notebook_Start_Demo)

    # DimAudit_Start:
    DimAudit_Start(client_code, vendor, Notebook_Start_Demo,None ,None ,None)

    #DimAudit_Start
    import pyodbc

    # Define your connection parameters
    server = '10.20.30.40'
    database = 'VI_Stuff'
    username = 'WQELPRES_'
    password = '$g7Xv!QzR9m@L2e#T8bW^uYpK4s&JhNcVxAoZqMd'
    conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Connect to the SQL Server database
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()

    # Get parameters from Databricks notebook
    start_Time = Notebook_Start_Demo
    #source_file_path = 'NULL'  
    table_name = client_code + '_' + vendor 
    PkgName = notebook_name
    DimAudit = 'dbo.' + vendor + '_' + 'DimAudit'

    # Define and execute your SQL query using Pyodbc syntax
    query = '''
    DECLARE @curDate DATETIME
    DECLARE @TableName VARCHAR(MAX)
    DECLARE @PkgName VARCHAR(MAX)

    SET @curDate = ?
    SET @TableName = ?
    SET @PkgName = ?

    INSERT INTO ''' + DimAudit + '''
               (ParentAuditKey
               ,[TableName]
               ,[PkgName]       
               ,[ExecStartDT]
               ,[ExecStopDT]           
               ,[ExtractRowCnt]
               ,[InsertRowCnt]   
               ,[UpdateRowCnt]                  
               ,[ErrorRowCnt]
               ,[TableInitialRowCnt]
               ,[TableFinalRowCnt]
               ,[TableMaxDateTime]
               ,[SuccessfulProcessingInd]
               ,[SourceFilePath])
         VALUES
               (1
               ,@TableName
               ,@PkgName       
               ,@curDate
               ,NULL           
               ,NULL
               ,0
               ,0          
               ,NULL
               ,0
               ,0
               ,NULL
               ,'N'
               ,NULL)
    '''
    cursor.execute(query,start_Time, table_name,PkgName)

    # Commit the transaction
    conn.commit()

    # Fetch the results of the SELECT statement
    Max_AuditKey = "SELECT max(Auditkey) FROM " + DimAudit + " WHERE [ExecStartDT] = ? and [TableName] = ?"
    select_cursor = conn.cursor()
    select_cursor.execute(Max_AuditKey, start_Time,  table_name)
    results = select_cursor.fetchone()

    # Print the fetched results
    for row in results:
        AuditKey = row
    print(AuditKey)

    file_name = client_code +'_' + vendor.capitalize() + '_' + 'PBCDM'
    df = Get_vw_PBCDM(database_name)
    row_count = df.count()
    RunIdeationLocal_txt(RunDate,'|', file_extension, quote_option)

    # DimAudit_Update:
    output_file_path = lake_destination_full_path + file_name + output_file_name
    Notebook_Stop_Demo = get_current_timestamp_est()
    DimAudit_Update(Notebook_Start_Demo,Notebook_Stop_Demo,row_count, output_file_path)


    #DimAudit_End
    import pyodbc

    # Get the current date and time in UTC
    UTC_End = datetime.now(pytz.utc)

    # Convert UTC time to Eastern Standard Time (EST)
    est = pytz.timezone('US/Eastern')
    EST_End = UTC_End.astimezone(est)

    # Get the current date and time in EST with nanoseconds
    Notebook_End = EST_End.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    print(Notebook_End)

    # Define your connection parameters
    server = '10.20.30.40'
    database = 'VI_Stuff'
    username = 'WQELPRES_'
    password = '$g7Xv!QzR9m@L2e#T8bW^uYpK4s&JhNcVxAoZqMd'
    conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'


    Parent_AuditKey = AuditKey
    #print(Parent_AuditKey)
    ExtractRowCount_DimAudit = row_count

    # Define your SQL query separately
    query = '''
        UPDATE ''' + DimAudit + '''
        SET [ExecStopDT] = ?,
            ExtractRowCnt = ?,
            SuccessfulProcessingInd = 'Y',
            TableFinalRowCnt = ?,
            SourceFilePath = ?,
            ParentAuditKey = ?
        WHERE AuditKey = ? AND [TableName] = ? AND [ExecStartDT] = ?
    '''

    #exit()
    # Execute the query
    cursor.execute(query, (Notebook_End, ExtractRowCount_DimAudit, ExtractRowCount_DimAudit,output_file_path,Parent_AuditKey, Parent_AuditKey, table_name,start_Time))

    # Commit the transaction
    conn.commit()

    # Close the connection
    conn.close()


# COMMAND ----------

#3 CostCenters File  , Frequency - weekly
# FileName: YYYYMMDD_YYYYMMDD_RoperStFrancis_vw_CC_YYYYMMDD.txt  

if monthly_weekly.lower() == 'weekly':

    Notebook_Start_Demo = get_current_timestamp_est()
    print(Notebook_Start_Demo)

    # DimAudit_Start:
    DimAudit_Start(client_code, vendor, Notebook_Start_Demo,None ,None ,None)

    #DimAudit_Start
    import pyodbc

    # Define your connection parameters
    server = '10.20.30.40'
    database = 'VI_Stuff'
    username = 'WQELPRES_'
    password = '$g7Xv!QzR9m@L2e#T8bW^uYpK4s&JhNcVxAoZqMd'
    conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Connect to the SQL Server database
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()

    # Get parameters from Databricks notebook
    start_Time = Notebook_Start_Demo
    #source_file_path = 'NULL'  
    table_name = client_code + '_' + vendor 
    PkgName = notebook_name
    DimAudit = 'dbo.' + vendor + '_' + 'DimAudit'

    # Define and execute your SQL query using Pyodbc syntax
    query = '''
    DECLARE @curDate DATETIME
    DECLARE @TableName VARCHAR(MAX)
    DECLARE @PkgName VARCHAR(MAX)

    SET @curDate = ?
    SET @TableName = ?
    SET @PkgName = ?

    INSERT INTO ''' + DimAudit + '''
               (ParentAuditKey
               ,[TableName]
               ,[PkgName]       
               ,[ExecStartDT]
               ,[ExecStopDT]           
               ,[ExtractRowCnt]
               ,[InsertRowCnt]   
               ,[UpdateRowCnt]                  
               ,[ErrorRowCnt]
               ,[TableInitialRowCnt]
               ,[TableFinalRowCnt]
               ,[TableMaxDateTime]
               ,[SuccessfulProcessingInd]
               ,[SourceFilePath])
         VALUES
               (1
               ,@TableName
               ,@PkgName       
               ,@curDate
               ,NULL           
               ,NULL
               ,0
               ,0          
               ,NULL
               ,0
               ,0
               ,NULL
               ,'N'
               ,NULL)
    '''
    cursor.execute(query,start_Time, table_name,PkgName)

    # Commit the transaction
    conn.commit()

    # Fetch the results of the SELECT statement
    Max_AuditKey = "SELECT max(Auditkey) FROM " + DimAudit + " WHERE [ExecStartDT] = ? and [TableName] = ?"
    select_cursor = conn.cursor()
    select_cursor.execute(Max_AuditKey, start_Time,  table_name)
    results = select_cursor.fetchone()

    # Print the fetched results
    for row in results:
        AuditKey = row
    print(AuditKey)

    #Create Date Parameters
    dt1 = '2023/01/01'
    dt1FN = '20230101'
    dt2 = (EST_Start - timedelta(1)).strftime('%Y/%m/%d') # current date in YYYY/MM/DD
    dt2FN = (EST_Start - timedelta(1)).strftime('%Y%m%d')  # current date in YYYYMMDD

    file_name = dt1FN + '_' + dt2FN + '_' + client_code +'_' + vendor.capitalize() + '_' + 'CC'
    df = Get_vw_CostCenters(database_name)
    row_count = df.count()
    RunIdeationLocal_txt(RunDate,'|', file_extension, quote_option)

    # DimAudit_Update:
    output_file_path = lake_destination_full_path + file_name + output_file_name
    Notebook_Stop_Demo = get_current_timestamp_est()
    DimAudit_Update(Notebook_Start_Demo,Notebook_Stop_Demo,row_count, output_file_path)


    #DimAudit_End
    import pyodbc

    # Get the current date and time in UTC
    UTC_End = datetime.now(pytz.utc)

    # Convert UTC time to Eastern Standard Time (EST)
    est = pytz.timezone('US/Eastern')
    EST_End = UTC_End.astimezone(est)

    # Get the current date and time in EST with nanoseconds
    Notebook_End = EST_End.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    print(Notebook_End)

    # Define your connection parameters
    server = '10.20.30.40'
    database = 'VI_Stuff'
    username = 'WQELPRES_'
    password = '$g7Xv!QzR9m@L2e#T8bW^uYpK4s&JhNcVxAoZqMd'
    conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'


    Parent_AuditKey = AuditKey
    #print(Parent_AuditKey)
    ExtractRowCount_DimAudit = row_count

    # Define your SQL query separately
    query = '''
        UPDATE ''' + DimAudit + '''
        SET [ExecStopDT] = ?,
            ExtractRowCnt = ?,
            SuccessfulProcessingInd = 'Y',
            TableFinalRowCnt = ?,
            SourceFilePath = ?,
            ParentAuditKey = ?
        WHERE AuditKey = ? AND [TableName] = ? AND [ExecStartDT] = ?
    '''

    #exit()
    # Execute the query
    cursor.execute(query, (Notebook_End, ExtractRowCount_DimAudit, ExtractRowCount_DimAudit,output_file_path,Parent_AuditKey, Parent_AuditKey, table_name,start_Time))

    # Commit the transaction
    conn.commit()

    # Close the connection
    conn.close()


# COMMAND ----------

#4 HBRevUsage File  
# FileName: YYYYMMDD_YYYYMMDD_RoperStFrancis_vw_HBREVUSAGE_YYYYMMDD.txt  

if monthly_weekly.lower() == 'monthly':

    Notebook_Start_Demo = get_current_timestamp_est()
    print(Notebook_Start_Demo)

    # DimAudit_Start:
    DimAudit_Start(client_code, vendor, Notebook_Start_Demo,None ,None ,None)

    #DimAudit_Start
    import pyodbc

    # Define your connection parameters
    server = '10.20.30.40'
    database = 'VI_Stuff'
    username = 'WQELPRES_'
    password = '$g7Xv!QzR9m@L2e#T8bW^uYpK4s&JhNcVxAoZqMd'
    conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Connect to the SQL Server database
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()

    # Get parameters from Databricks notebook
    start_Time = Notebook_Start_Demo
    #source_file_path = 'NULL'  
    table_name = client_code + '_' + vendor 
    PkgName = notebook_name
    DimAudit = 'dbo.' + vendor + '_' + 'DimAudit'

    # Define and execute your SQL query using Pyodbc syntax
    query = '''
    DECLARE @curDate DATETIME
    DECLARE @TableName VARCHAR(MAX)
    DECLARE @PkgName VARCHAR(MAX)

    SET @curDate = ?
    SET @TableName = ?
    SET @PkgName = ?

    INSERT INTO ''' + DimAudit + '''
               (ParentAuditKey
               ,[TableName]
               ,[PkgName]       
               ,[ExecStartDT]
               ,[ExecStopDT]           
               ,[ExtractRowCnt]
               ,[InsertRowCnt]   
               ,[UpdateRowCnt]                  
               ,[ErrorRowCnt]
               ,[TableInitialRowCnt]
               ,[TableFinalRowCnt]
               ,[TableMaxDateTime]
               ,[SuccessfulProcessingInd]
               ,[SourceFilePath])
         VALUES
               (1
               ,@TableName
               ,@PkgName       
               ,@curDate
               ,NULL           
               ,NULL
               ,0
               ,0          
               ,NULL
               ,0
               ,0
               ,NULL
               ,'N'
               ,NULL)
    '''
    cursor.execute(query,start_Time, table_name,PkgName)

    # Commit the transaction
    conn.commit()

    # Fetch the results of the SELECT statement
    Max_AuditKey = "SELECT max(Auditkey) FROM " + DimAudit + " WHERE [ExecStartDT] = ? and [TableName] = ?"
    select_cursor = conn.cursor()
    select_cursor.execute(Max_AuditKey, start_Time,  table_name)
    results = select_cursor.fetchone()

    # Print the fetched results
    for row in results:
        AuditKey = row
    print(AuditKey)

    #Create Date Parameters
    dt1 = '2023/01/01'
    dt1FN = '20230101'
    dt2 = (EST_Start - timedelta(1)).strftime('%Y/%m/%d') # current date in YYYY/MM/DD
    dt2FN = (EST_Start - timedelta(1)).strftime('%Y%m%d')  # current date in YYYYMMDD

    file_name = dt1FN + '_' + dt2FN + '_' + client_code +'_' + vendor.capitalize() + '_' + 'HBREVUSAGE'
    df = Get_vw_HBRevUsage(database_name,dt1, dt2)
    row_count = df.count()
    RunIdeationLocal_txt(RunDate,'|', file_extension, quote_option)

    # DimAudit_Update:
    output_file_path = lake_destination_full_path + file_name + output_file_name
    Notebook_Stop_Demo = get_current_timestamp_est()
    DimAudit_Update(Notebook_Start_Demo,Notebook_Stop_Demo,row_count, output_file_path)


    #DimAudit_End
    import pyodbc

    # Get the current date and time in UTC
    UTC_End = datetime.now(pytz.utc)

    # Convert UTC time to Eastern Standard Time (EST)
    est = pytz.timezone('US/Eastern')
    EST_End = UTC_End.astimezone(est)

    # Get the current date and time in EST with nanoseconds
    Notebook_End = EST_End.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    print(Notebook_End)

    # Define your connection parameters
    server = '10.20.30.40'
    database = 'VI_Stuff'
    username = 'WQELPRES_'
    password = '$g7Xv!QzR9m@L2e#T8bW^uYpK4s&JhNcVxAoZqMd'
    conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'


    Parent_AuditKey = AuditKey
    #print(Parent_AuditKey)
    ExtractRowCount_DimAudit = row_count

    # Define your SQL query separately
    query = '''
        UPDATE ''' + DimAudit + '''
        SET [ExecStopDT] = ?,
            ExtractRowCnt = ?,
            SuccessfulProcessingInd = 'Y',
            TableFinalRowCnt = ?,
            SourceFilePath = ?,
            ParentAuditKey = ?
        WHERE AuditKey = ? AND [TableName] = ? AND [ExecStartDT] = ?
    '''

    #exit()
    # Execute the query
    cursor.execute(query, (Notebook_End, ExtractRowCount_DimAudit, ExtractRowCount_DimAudit,output_file_path,Parent_AuditKey, Parent_AuditKey, table_name,start_Time))

    # Commit the transaction
    conn.commit()

    # Close the connection
    conn.close()


# COMMAND ----------

#5 PBRevUsage File  - Frequency  monthly
# FileName: YYYYMMDD_YYYYMMDD_RoperStFrancis_vw_PBREVUSAGE_YYYYMMDD.txt  

if monthly_weekly.lower() == 'monthly':

    Notebook_Start_Demo = get_current_timestamp_est()
    print(Notebook_Start_Demo)

    # DimAudit_Start:
    DimAudit_Start(client_code, vendor, Notebook_Start_Demo,None ,None ,None)

    #DimAudit_Start
    import pyodbc

    # Define your connection parameters
    server = '10.20.30.40'
    database = 'VI_Stuff'
    username = 'WQELPRES_'
    password = '$g7Xv!QzR9m@L2e#T8bW^uYpK4s&JhNcVxAoZqMd'
    conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Connect to the SQL Server database
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()

    # Get parameters from Databricks notebook
    start_Time = Notebook_Start_Demo
    #source_file_path = 'NULL'  
    table_name = client_code + '_' + vendor 
    PkgName = notebook_name
    DimAudit = 'dbo.' + vendor + '_' + 'DimAudit'

    # Define and execute your SQL query using Pyodbc syntax
    query = '''
    DECLARE @curDate DATETIME
    DECLARE @TableName VARCHAR(MAX)
    DECLARE @PkgName VARCHAR(MAX)

    SET @curDate = ?
    SET @TableName = ?
    SET @PkgName = ?

    INSERT INTO ''' + DimAudit + '''
               (ParentAuditKey
               ,[TableName]
               ,[PkgName]       
               ,[ExecStartDT]
               ,[ExecStopDT]           
               ,[ExtractRowCnt]
               ,[InsertRowCnt]   
               ,[UpdateRowCnt]                  
               ,[ErrorRowCnt]
               ,[TableInitialRowCnt]
               ,[TableFinalRowCnt]
               ,[TableMaxDateTime]
               ,[SuccessfulProcessingInd]
               ,[SourceFilePath])
         VALUES
               (1
               ,@TableName
               ,@PkgName       
               ,@curDate
               ,NULL           
               ,NULL
               ,0
               ,0          
               ,NULL
               ,0
               ,0
               ,NULL
               ,'N'
               ,NULL)
    '''
    cursor.execute(query,start_Time, table_name,PkgName)

    # Commit the transaction
    conn.commit()

    # Fetch the results of the SELECT statement
    Max_AuditKey = "SELECT max(Auditkey) FROM " + DimAudit + " WHERE [ExecStartDT] = ? and [TableName] = ?"
    select_cursor = conn.cursor()
    select_cursor.execute(Max_AuditKey, start_Time,  table_name)
    results = select_cursor.fetchone()

    # Print the fetched results
    for row in results:
        AuditKey = row
    print(AuditKey)

    #Create Date Parameters
    dt1 = '2023/01/01'
    dt1FN = '20230101'
    dt2 = (EST_Start - timedelta(1)).strftime('%Y/%m/%d') # current date in YYYY/MM/DD
    dt2FN = (EST_Start - timedelta(1)).strftime('%Y%m%d')  # current date in YYYYMMDD

    file_name = dt1FN + '_' + dt2FN + '_' + client_code +'_' + vendor.capitalize() + '_' + 'PBREVUSAGE'
    df = Get_vw_PBRevUsage(database_name_prc)
    row_count = df.count()
    RunIdeationLocal_txt(RunDate,'|', file_extension, quote_option)

    # DimAudit_Update:
    output_file_path = lake_destination_full_path + file_name + output_file_name
    Notebook_Stop_Demo = get_current_timestamp_est()
    DimAudit_Update(Notebook_Start_Demo,Notebook_Stop_Demo,row_count, output_file_path)


    #DimAudit_End
    import pyodbc

    # Get the current date and time in UTC
    UTC_End = datetime.now(pytz.utc)

    # Convert UTC time to Eastern Standard Time (EST)
    est = pytz.timezone('US/Eastern')
    EST_End = UTC_End.astimezone(est)

    # Get the current date and time in EST with nanoseconds
    Notebook_End = EST_End.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    print(Notebook_End)

    # Define your connection parameters
    server = '10.20.30.40'
    database = 'VI_Stuff'
    username = 'WQELPRES_'
    password = '$g7Xv!QzR9m@L2e#T8bW^uYpK4s&JhNcVxAoZqMd'
    conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'


    Parent_AuditKey = AuditKey
    #print(Parent_AuditKey)
    ExtractRowCount_DimAudit = row_count

    # Define your SQL query separately
    query = '''
        UPDATE ''' + DimAudit + '''
        SET [ExecStopDT] = ?,
            ExtractRowCnt = ?,
            SuccessfulProcessingInd = 'Y',
            TableFinalRowCnt = ?,
            SourceFilePath = ?,
            ParentAuditKey = ?
        WHERE AuditKey = ? AND [TableName] = ? AND [ExecStartDT] = ?
    '''

    #exit()
    # Execute the query
    cursor.execute(query, (Notebook_End, ExtractRowCount_DimAudit, ExtractRowCount_DimAudit,output_file_path,Parent_AuditKey, Parent_AuditKey, table_name,start_Time))

    # Commit the transaction
    conn.commit()

    # Close the connection
    conn.close()


# COMMAND ----------


