##-- Libraries and Packages

##-- Functions
def table_creation(table_name, query_path, conn):
    cursor = conn.cursor()    
    try:
        cursor.execute(f"DROP TABLE {table_name}")  
        print(f'{table_name} has been eliminated from DB')
    except:
        print(f'{table_name} is not in the DB')
        
    finally:        
        cursor.execute(open(query_path).read())
        print(f'{table_name} has been created in the DB')
    

