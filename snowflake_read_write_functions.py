import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas, pd_writer

from edl_write_config import EDL_CONN_INFO  # these are your snowkflake credentials (read/ write depending on function)
# EDL = Enterprise Data Lake
# EDL_Data_column_names : names of the columns in the EDL table

def get_sflake_connector():
    ctx = snowflake.connector.connect(
        user=EDL_CONN_INFO['USER'],
        password=EDL_CONN_INFO['PASSWORD'],
        account=EDL_CONN_INFO['ACCOUNT'],
        warehouse=EDL_CONN_INFO['WAREHOUSE'],
        database=EDL_CONN_INFO['DATABASE'],
        schema=EDL_CONN_INFO['SCHEMA']
        )
    return ctx

def get_sflake_engine():
    engine = create_engine(
        'snowflake://{user}:{password}@{account}/{db}/{schema}?warehouse={warehouse}&role={role_name}'.format(
            user=EDL_CONN_INFO['USER'],
            password=EDL_CONN_INFO['PASSWORD'],
            account=EDL_CONN_INFO['ACCOUNT'],
            db=EDL_CONN_INFO['DATABASE'],
            schema=EDL_CONN_INFO['SCHEMA'],
            warehouse=EDL_CONN_INFO['WAREHOUSE'],
            role_name = EDL_CONN_INFO['ROLE']
        )
    )
    return engine

def write_to_snowflake(sflake_ctx, write_data):
    # success, nchunks, nrows, _ = write_pandas(sflake_ctx, write_data, EDL_CONN_INFO['TABLE'])
    write_data = write_data[EDL_Data_column_names]
    m_success = write_data.to_sql(EDL_CONN_INFO['TABLE'], con=sflake_ctx, index=False, method=pd_writer, if_exists="append")
    return m_success

def format_prior_edl_push(t_df, col_name):
    t_df[col_name] = t_df[col_name].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

# this method attempts to store df as csv, stage, and then push to snowflake table. this is done to test the speed, as well as the timestamp pushing issues.
def alternative_method(sflake_ctx, t_df, table_name):
    try:
        t_df.to_csv(file_dest, index=False, header=False)
        sflake_cs = sflake_ctx.cursor()
        sflake_cs.execute(f"put 'file://{file_dest}' @%{table_name}")
        sflake_cs.execute(f"copy into {table_name}")
        sflake_cs.close()
        return True
    except Exception as e:
        return e

# read from snowflake:
def get_sfl_transition_data(query):
    ctx = get_sflake_connector()
    s_cursor = ctx.cursor()
    s_cursor.execute(query)
    transition_df = s_cursor.fetch_pandas_all()
    s_cursor.close()
    return transition_df

s_eng = get_sflake_engine()
write_out = write_to_snowflake(s_eng,test_df)