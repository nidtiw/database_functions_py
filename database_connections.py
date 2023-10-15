import pymysql
from config import MYSQL_CONN_INFO

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