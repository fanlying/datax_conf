"""
仅用于datax从MySQL和Oracle往hive中抽数使用
生成和源表结构一致的hive建表语句文件并在hive中建表
生成datax配置json文件
"""
import pymysql
import cx_Oracle as oracle
import pandas as pd
import os
import json
import sys

class datax_db_2_hive(object):
    """docstring for datax_db_2_hive
    connect_id为配置文件的id，根据id取数据库连接
    table =  为mysql、Oracle表名，hive表名需加前缀
    schema = 为数据库中的库名，MySQL中schema和库名一致，此参数用于Oracle
    """
    conf_file = pd.read_table(r'F:\code\ods\etl\conf_file',index_col='id')
    def __init__(self, connect_id, schema, table):
        super(datax_db_2_hive, self).__init__()
        self.connect_id = connect_id
        self.hostname = datax_db_2_hive.conf_file.host[connect_id]
        self.username = datax_db_2_hive.conf_file.username[connect_id]
        self.password = datax_db_2_hive.conf_file.password[connect_id]
        self.db = datax_db_2_hive.conf_file.db[connect_id]
        self.port = str(datax_db_2_hive.conf_file.port[connect_id])
        self.dbtype = datax_db_2_hive.conf_file.db_type[connect_id]
        self.prefix = datax_db_2_hive.conf_file.prefix[connect_id]
        self.path = datax_db_2_hive.conf_file.path[connect_id]
        self.schema = schema
        self.table = table
        
    def get_mysql_info(self, mysqlcharset='utf8'): 
        """
        从MySQL元数据获取抽数配置信息
        """
        connection=pymysql.connect(host = self.hostname,
                                   user = self.username,
                                   password = self.password,
                                   db = self.db,
                                   port = int(self.port),
                                   charset = mysqlcharset
                                   )
        cols = []
        create_body=''
        query_str='select '
        try:
        #获取一个游标
            with connection.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
                sql='SHOW FULL FIELDS FROM  {0}'.format(self.table)
                cnt=cursor.execute(sql) #返回记录条数
                try:
                    for row in cursor:#cursor.fetchall()
                        #print(row)
                        if row['Type'].split('(')[0] in ('int', 'tinyint', 'smallint', 'mediumint', 'integer'):
                            row['Type'] = "int"
                            row['writeType'] = "string"
                        elif 'bigint' in row['Type']:
                            row['Type'] = "bigint"
                            row['writeType'] = "string"
                        elif row['Type'].split('(')[0] in ('double','float'):
                            row['Type'] = "double"
                            row['writeType'] = "double"
                        elif 'decimal' in row['Type']:
                            row['Type'] = row['Type']
                            row['writeType'] = "double"
                        else:
                            row['Type'] = "string"
                            row['writeType'] = "string"
                        create_body += row['Field'] + ' '+ row['Type'] +' comment \'' + row['Comment'] + '\' ,\n'
                        query_str += row['Field'] + ','
                        coljson = eval('{"name":"' + row['Field'] + '","type":"' + row['writeType'] + '"}')
                        # print(coljson)
                        cols.append(coljson)
                        # print(cols)
                except Exception as e:
                    print('程序异常!')  
                    raise e
                comment_sql = "SELECT t2.TABLE_COMMENT FROM information_schema.TABLES t2 WHERE t2.table_schema = lower('{0}') and t2.table_name = lower('{1}')".format(self.db,self.table)
                cursor.execute(comment_sql) 
                tablecomment = cursor.fetchone()['TABLE_COMMENT']
        finally:
            connection.close()
        # create_body += 'etl_time string comment \'etl时间\') \ncomment \'%s\''%tablecomment
        # query_str += 'etl_time from {0}.{1}'.format(self.db,self.table)
        # cols.append(eval('{"name":"etl_time","type":"string"}'))
        return create_body, query_str, cols, tablecomment


    def get_oracle_info(self): 
        """
        从oracle元数据获取抽数配置信息
        """
        # connect oracle database
        connect_str = self.username + '/' + self.password + '@' + self.hostname + ':' + self.port + '/' + self.db
        connection = oracle.connect(connect_str, encoding = "UTF-8", nencoding = "UTF-8")
         
        # create cursor
        # cursor = connection.cursor()

        cols = []
        create_body=''
        query_str='select '
        try:
        #获取一个游标
            with connection.cursor() as cursor:
                sql="select T.COLUMN_NAME,T.COMMENTS,C.DATA_TYPE,C.DATA_PRECISION,C.DATA_SCALE from all_COL_COMMENTS t, all_TAB_COLUMNS c where c.column_name = t.column_name and c.owner = t.owner and c.TABLE_NAME = t.TABLE_NAME and c.owner = upper('{0}') and c.TABLE_NAME = upper('{1}') order by c.COLUMN_ID".format(self.schema, self.table)
                cursor.execute(sql)
                try:
                    for tp_row in cursor:#cursor.fetchall()
                        #print(row)
                        row = list(tp_row)
                        if row[2] == 'INTEGER':
                            row[2] = "bigint"
                            row.append("string")
                        elif row[2] == 'NUMBER':
                            if row[4] != 0:
                                row[2] = "decimal(" + str(row[3]) + ',' + str(row[4]) + ')'
                                row.append("double")
                            elif row[3] <= 11:
                                row[2] = 'int'
                                row.append('string')
                            else:
                                row[2] = 'bigint'
                                row.append('string')                           
                        elif row[2] in ('BINARY_FLOAT', 'BINARY_DOUBLE', 'FLOAT'):
                            row[2] = "double"
                            row.append("double")
                        else:
                            row[2] = "string"
                            row.append("string")
                        create_body += row[0] + ' '+ row[2] +' comment \'' + str(row[1]) + '\' ,\n'
                        query_str += row[0] + ','
                        coljson = eval('{"name":"' + row[0] + '","type":"' + row[5] + '"}')
                        # print(coljson)
                        cols.append(coljson)
                        # print(cols)
                except Exception as e:
                    print('程序异常!')  
                    raise e
                comment_sql = "select t.comments from all_tab_comments t where owner = upper('{0}') and table_name = upper('{1}')".format(self.schema,self.table)
                cursor.execute(comment_sql) 
                tablecomment = cursor.fetchone()
        finally:
            connection.close()
        # create_body += 'etl_time string comment \'etl时间\') \ncomment \'%s\''%tablecomment
        # query_str += 'etl_time from {0}.{1}'.format(self.db,self.table)
        # cols.append(eval('{"name":"etl_time","type":"string"}'))
        return create_body, query_str, cols, tablecomment


    def dumpjson(self, query_sql, cols):
        """
        生成配置json文件
        """
        f = open(r'F:\code\ods\etl\datax_db_2_hive.json', encoding='utf-8')
        setting = json.load(f, strict=False)
        #json文件配置项
        setting['job']['content'][0]['reader']["name"] = self.dbtype + 'reader'
        setting['job']['content'][0]['reader']['parameter']['password'] = self.password
        setting['job']['content'][0]['reader']['parameter']['username'] = self.username
        setting['job']['content'][0]['reader']['parameter']['connection'][0]['querySql'][0] = query_sql
        if self.dbtype == 'mysql':
            jdbc = 'jdbc:mysql://' + self.hostname + ':' + self.port + '/' + self.db + '?useUnicode=true&characterEncoding=UTF8&tinyInt1isBit=false'
        elif self.dbtype == 'oracle':
            jdbc = 'jdbc:oracle:thin:@' + self.hostname + ':' + self.port + '/' + self.db
            pass
        setting['job']['content'][0]['reader']['parameter']['connection'][0]['jdbcUrl'][0] = jdbc
        
        setting['job']['content'][0]['writer']['parameter']['column'] = cols
        setting['job']['content'][0]['writer']['parameter']['path'] = '/user/hive/warehouse/bigdata_ods.db/ods_' + self.prefix + '_' + self.table + '/'
        setting['job']['content'][0]['writer']['parameter']['fileName'] = 'ods_' + self.prefix + '_' + self.table
        jsObj = json.dumps(setting)
        write_json_path = 'F:\\code\\ods\\' + self.path + '\\' + 'ods_' + self.prefix + '_' + self.table
        if not os.path.exists(write_json_path):
            os.makedirs(write_json_path)
        write_path_json = write_json_path + '\\' + 'ods_' + self.prefix + '_' + self.table + '.json'
        with open(write_json_path, "w") as f:    
            f.write(jsObj)    
            f.close()
        return print('已生成json文件：', write_json_path)

    def create_hive_table(self, ispartition = False):
        '''
        ispartition : 是否分区默认为分区
        '''
    
        create_head = '''
create table if not exists bigdata_ods.ods_{0}_{1}('''.format(self.prefix,self.table)
    
        if ispartition:
            create_tail = r'''
partitioned by (ds string comment '分区日期')
row format delimited fields terminated by '\001';'''
        else:
            create_tail = r'''
row format delimited fields terminated by '\001';'''
    
        if self.dbtype == 'mysql':
            create_body, query_str, cols, tablecomment = datax_db_2_hive.get_mysql_info(self)
            query_str += 'current_timestamp as etl_time from {0}.{1}'.format(self.db,self.table)
        elif self.dbtype == 'oracle':
            create_body, query_str, cols, tablecomment = datax_db_2_hive.get_oracle_info(self)
            query_str += 'sysdate as etl_time from {0}.{1}'.format(self.schema,self.table)

        create_body += 'etl_time string comment \'etl时间\') \ncomment \'%s\''%tablecomment
        cols.append(eval('{"name":"etl_time","type":"string"}'))

        datax_db_2_hive.dumpjson(self, query_str, cols)
        create_str = create_head + '\n' + create_body + create_tail
        write_create_path = 'F:\\code\\ods\\' + self.path + '\\' + 'ods_' + self.prefix + '_' + self.table
        if not os.path.exists(write_create_path):
            os.makedirs(write_create_path)
        write_create_sql = write_create_path + '\\create_ods_' + self.prefix + '_' + self.table + '.sql'
        with open(write_create_sql, "w") as f:    
            f.write(create_str)    
            f.close()
        os.popen("hive -f %s" %write_create_sql)
        # print("hive -f %s" %write_create_sql)
        return print('已生成建表语句文件并开始执行：', write_create_sql)

def main():
    if len(sys.argv) == 4:
        try:
            argv1 = int(sys.argv[1])
            #datax_db_2_hive(52, 'rpt', 'dim_big_cate').create_hive_table()
            #datax_db_2_hive(51, 'basis', 'big_cate_day').create_hive_table()
            datax_db_2_hive(argv1, sys.argv[2], sys.argv[3]).create_hive_table()
        except Exception as e:
            raise e
    elif len(sys.argv) == 5:
        try:
            argv1 = int(sys.argv[1])
            #datax_db_2_hive(52, 'rpt', 'dim_big_cate').create_hive_table()
            #datax_db_2_hive(51, 'basis', 'big_cate_day').create_hive_table()
            datax_db_2_hive(argv1, sys.argv[2], sys.argv[3]).create_hive_table(sys.argv[4])
        except Exception as e:
            raise e
    elif len(sys.argv) == 2 and sys.argv[1].upper() in ('HELP', 'H'):
        print('传入参数为：conf_file中的id schema table [是否分区 = False]')
    else:
        print('请传入正确的参数')

if __name__ == '__main__':
    main()