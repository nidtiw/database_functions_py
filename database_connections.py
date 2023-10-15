import pymysql
from sqlalchemy import create_engine
import mysql.connector
from config import MYSQL_CONN_INFO  # import your credentials from a config file

def connect_database_server(mysql_queries):
    connection = pymysql.connect(host=MYSQL_CONN_INFO['host'],
        port=MYSQL_CONN_INFO['port'],
        user=MYSQL_CONN_INFO['user'],
        password=MYSQL_CONN_INFO['password'],
        charset='utf8mb4',
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as c:
            if c.connection:
                print('Connection Open')
            else:
                print('PyMySQL Connection broken!')
            query_list = []
            try:
                for i in mysql_queries:
                    c.execute(i)
                    query_list.append(c.fetchall())
                lite_df = pd.DataFrame(query_list[0])
    # closing connection
    connection.close()

# append a dataframe to a mysql table using sqlalchemy
def append2db(df, table_name, column_names, conn_info):
    db_ = mysql.connector.connect(host=conn_info['host'],user=conn_info['user'],password=conn_info['password'],database=conn_info['schema'])

    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{database}"
                           .format(user=conn_info['user'],
                                   pw=conn_info['password'],
                                   database=conn_info['schema']))
    
    print('before df insert:{}'.format(df.head()))
    df.insert(loc=0, column='id', value=None)
    df = df[column_names]
    df.to_sql(table_name, con=engine, if_exists='append', chunksize=5000, index=False)
    db_.commit()

def create_pymysql_connection(connInfo):
    connection = pymysql.connect(host=connInfo['host'],
		port=3306,
		user=connInfo['user'],
		password=connInfo['password'],
		db=connInfo['schema'],
		charset='utf8mb4',
		autocommit=True,
		cursorclass=pymysql.cursors.DictCursor)

# read from local mysql table
def execute_db_command(command, connInfo):
	connection = create_pymysql_connection(connInfo)

	with connection.cursor() as cursor:
		cursor.execute(command)
		rows = cursor.fetchall()
		return rows

# write (update, insert) to local mysql table
def execute_db_change_command(sql_command, connInfo):
	connection = create_pymysql_connection(connInfo)

	with connection.cursor() as cursor:
                        cursor.execute(sql_command)