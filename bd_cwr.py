import psycopg2


def connect_bd(db='postgres', user='postgres', passw='', host="127.0.0.1", port="5432"):
    conn = None
    try:
        conn = psycopg2.connect(database=db, user=user, password=passw, host=host, port=port)
        cur = conn.cursor()
        
    except (Exception, psycopg2.DatabaseError) as error: 
        print("Error while creating table", error)  
    return conn, cur 


def create_db(conn, cur, name_bd):
    try:
        str_cmd = f'CREATE DATABASE {name_bd}'
        cur.execute(str_cmd)
    except psycopg2.OperationalError as error:
        print(f'The error {error} occured')  
    conn.commit()

def create_tab(conn, cur, name_table, **kwargs):
    try:
        par_col = ''.join(f'{name} {val},' for name, val in kwargs.items())
        str_cmd = f'CREATE TABLE {name_table}'
        cur.execute('CREATE TABLE test1 (id INT PRIMARY KEY, name VARCHAR(10), salary INT, dept INT)') 
    except psycopg2.DatabaseError as error:
        print(f'Table {error}.')
    conn.commit()

def insert_in_table(con, cur, id = 1, name = '', salary = 1000, dept = 1):
    try:
        h = 'INSERT INTO {} VALUES(%s, %s, %s, %s);'.format('emp')
       
        cur.execute(h, (id, name, salary, dept))
    except Exception as error:
        print('error', error)
    con.commit()

def fetch_data(conn, cur):
    try:
        cur.execute('SELECT * FROM emp')
    except:
        print('error !')
    # сохранить результат в данных
    data = cur.fetchall()
    # вернуть результат
    return data 

if __name__ == "__main__":
    #con, cur = connect_bd(db="postgres", user="postgres", passw="vkraigor")
    ##create_db(con, 'vkmybd1')
    con, cur = connect_bd(db="vkmybd", user="postgres", passw="vkraigor")
    #create_db(con, cur, 'mydb')
    create_tab(con, cur, 'tab1')
    """ insert_in_table(con, cur, 6, 'adith', 1000, 2)

    insert_in_table(con, cur, 9, 'tyrion', 100000, 2)

    insert_in_table(con, cur, 10, 'jon', 100, 3)

    insert_in_table(con, cur, 11, 'daenerys', 10000, 4)""" 
    dat = fetch_data(con, cur)
    print(dat)

