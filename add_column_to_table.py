import pyodbc
def test_connection_with_DB():
    # Инициализируем курсор для подтверждения 
    cursor = connection.cursor()
    cursor.execute('SELECT TOP(2) * FROM Experiments')
    for row in cursor:
        print(row)
    
    cursor.execute('SELECT TOP(2) * FROM parameterSequence')
    for row in cursor:
        print(row)
        
connection = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                   "Server=USER-ПК\MSSQLSERVER01;"
                                   "Database=Analyze;"
                                   "Trusted_Connection=yes;")
test_connection_with_DB()
#ALTER TABLE table_name ADD COLUMN column_name data_type(precision);
connection.execute('ALTER TABLE parameterSequence ADD item int NULL;')
