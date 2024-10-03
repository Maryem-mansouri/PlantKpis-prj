from sqlalchemy import create_engine, MetaData, Table, update, select, and_
 
def establish_connectionAuth():
    conn_str = (
        r'mssql+pyodbc://MAN61NBO1VZ06Y2\SQLEXPRESS/AUTH'
        r'?driver=ODBC+Driver+17+for+SQL+Server'
        r'&trusted_connection=yes&MultipleActiveResultSets=True'
    )
    try:
        engine = create_engine(conn_str)
        connection = engine.connect()
        print("Connection established successfully.")
        return connection, engine
    except Exception as e:
        print(f"Error establishing connection: {e}")
        return None, None