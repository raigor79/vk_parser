import psycopg2


class BDError(Exception):
    pass


def connect_bd(db='postgres', user='postgres', passw='', host="127.0.0.1", port="5432"):
    conn = None
    try:
        conn = psycopg2.connect(database=db, user=user, password=passw, host=host, port=port)
        cur = conn.cursor()
        
    except (Exception, psycopg2.DatabaseError) as error: 
        raise BDError("Error while creating table -", error)  
    return conn, cur 


def create_tab(conn, cur, name_table, kwargs):
    try:
        par_col = ', '.join(f'{name} {val}' for name, val in kwargs.items())
        str_cmd = f'CREATE TABLE IF NOT EXISTS {name_table} ({par_col})'
        cur.execute(str_cmd)
    except psycopg2.DatabaseError as error:
        raise BDError("Error create table.")
    conn.commit()


def insert_in_table(con, cur, name_table, args):
    try:
        par_col = ','.join(f'%s' for _ in args)
        h = f'INSERT INTO {name_table} VALUES({par_col});'
        cur.execute(h, args)
    except Exception as error:
        raise BDError("Error insert in table.")
    con.commit()


def fetch_data(conn, cur, name_table):
    try:
        h = f'SELECT * FROM {name_table}'
        cur.execute(h)
    except:
       raise BDError("Error read from table.")
    data = cur.fetchall()
    return data 

def del_table(conn, cur, name_table):
    try:
        h = f"DROP TABLE {name_table}"
        cur.execute(h)
    except:
        raise BDError('Error delete table.')
    conn.commit()


if __name__ == "__main__":
    """primer"""
    #con, cur = connect_bd(db="vkmybd", user="postgres", passw="")
    #create_tab(con, cur, 'base_comm', {"id":"INT PRIMARY KEY", 'name':"VARCHAR(80)", "classification": 'VARCHAR(80)'})
    #insert_in_table(con, cur, 'loc_comm', [1334, 'юийска', 'None'])
    #dat1 = fetch_data(con, cur, 'base_comm')
    #dat2 = fetch_data(con, cur, 'aud_comm')
    #dat3 = fetch_data(con, cur, 'loc_comm')
    #del_table(con, cur, 'base_comm')
    #del_table(con, cur, 'aud_comm')
    #del_table(con, cur, 'loc_comm')