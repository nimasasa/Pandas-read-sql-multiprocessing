import pandas as pd
import jaydebeapi
import inspect, os
from datetime import datetime,timedelta

def connection(user, password ='<user_pass>'):
    mypath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) + '\JavaDrivers'
    conn = jaydebeapi.connect(jclassname="com.teradata.jdbc.TeraDriver",
                                           url="j<user_pass>",
                                           driver_args=[user, password],
                                           jars=[mypath+'/tdgssconfig.jar', mypath+'/terajdbc4.jar'])
    return conn

def queryGen(my_query,inputs):
    query = my_query.format(*inputs)
    return query

def read_chunks(inputs,query,user):  
    df = pd.read_sql_query(queryGen(query.read_query(),inputs), connection(user, password='<user_pass>'))
    return df

def date_range(start, end, intv):
    start = datetime.strptime(start,"%Y-%m-%d")
    end = datetime.strptime(end,"%Y-%m-%d")
    diff = (end  - start ) / intv
    for i in range(intv):
        if i==0:
            yield  ((start + diff * i).strftime("%Y-%m-%d") , (start + diff * (i+1)).strftime("%Y-%m-%d"))
        else:
            yield  ((start + diff * i+ timedelta(days=1)).strftime("%Y-%m-%d") , (start + diff * (i+1)).strftime("%Y-%m-%d"))