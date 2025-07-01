import psycopg2
from dbutils.pooled_db import PooledDB
from babySql.tools import PostgreSQLSelectConditionsBuilder, PostgreSQLCreateTable


class PostgreSQL:
    def __init__(self, host: str, port: int, user: str, passwd: str, db: str = None, max_connections: int = 50):
        """
        babySql for PostgreSQL
        :param host: 数据库地址
        :param port: 数据库端口
        :param user: 操作用户
        :param passwd: 用户密码
        :param db: 数据库名称
        :param max_connections: 最大连接数
        """
        self.__host__ = host
        self.__port__ = port
        self.__user__ = user
        self.__passwd__ = passwd
        self.__db__ = db
        self.__max_connections__ = max_connections
        self.__pool__ = PooledDB(
            creator=psycopg2,
            maxconnections=self.__max_connections__,
            **{
                'host': self.__host__,
                'port': self.__port__,
                'user': self.__user__,
                'password': self.__passwd__,
                'database': self.__db__
            }
        )

    def connect_information(self):
        """返回数据库连接信息"""
        return {
            "host": self.__host__,
            "port": self.__port__,
            "user": self.__user__,
            "passwd": self.__passwd__,
            "db": self.__db__
        }

    def user_defined_sql(self, sql: str, params: tuple = None):
        """执行自定义SQL"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        if params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
        row = cursor.fetchall()
        connect.commit()
        cursor.close()
        connect.close()
        return row

    def insert(self, table: str, columns: list, values: list):
        """插入数据"""
        if type(columns) is not list:
            raise TypeError(f"columns {columns} type is not list")
        if type(values) is not list:
            raise TypeError(f"values {columns} type is not list")
        column = "(" + ", ".join([f'"{col}"' for col in columns]) + ")"

        if type(values[0]) is list:
            params = ()
            value_list = []
            for value in values:
                if len(value) == len(columns):
                    for value_s in value:
                        params += (value_s,)
                    value_list.append("(" + ", ".join(["%s" for _ in value]) + ")")
                else:
                    raise ValueError(f"{columns}->{len(columns)} != {value}->{len(value)}")
            values = ", ".join(value_list)
            sql = f"insert into {table} {column} values {values};"
        else:
            params = ()
            if len(values) == len(columns):
                for value_s in values:
                    params += (value_s,)
                values = "(" + ", ".join(["%s" for _ in values]) + ")"
                sql = f"insert into {table} {column} values {values};"
            else:
                raise ValueError(f"{columns}->{len(columns)} != {values}->{len(values)}")

        connect = self.__pool__.connection()
        cursor = connect.cursor()
        cursor.execute(sql, params)
        connect.commit()
        cursor.close()
        connect.close()
        return sql

    def update(self, table: str, columns_values: dict):
        """更新数据"""
        if type(columns_values) is not dict:
            raise TypeError(f"columns_values {columns_values} type is not dict")
        cvs = ', '.join([f'"{k}"=\'{v}\'' for k, v in columns_values.items()])
        head_sql = f"UPDATE {table} SET {cvs} "
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        return PostgreSQLSelectConditionsBuilder(head_sql, cursor, connect)

    def delete(self, table: str):
        """删除数据"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        head_sql = f"DELETE FROM {table}"
        return PostgreSQLSelectConditionsBuilder(head_sql, cursor, connect)

    def select(self, table: str, columns: list = None):
        """查询数据"""
        if columns is not None and type(columns) is not list:
            raise TypeError(f"columns {columns} type is not list")
        if columns is None:
            columns_str = "*"
        else:
            columns_str = ", ".join([f'"{col}"' for col in columns])
        head_sql = f"SELECT {columns_str} FROM {table}"
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        return PostgreSQLSelectConditionsBuilder(head_sql, cursor, connect)

    def create_table(self, table_name, table_comment=None):
        """创建表"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        return PostgreSQLCreateTable(connect, cursor, table_name, table_comment=table_comment)

    def create_database(self, database_name, character="utf8", collate="utf8_general_ci"):
        """创建数据库"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        sql = f"CREATE DATABASE {database_name} WITH ENCODING='{character}' LC_COLLATE='{collate}' LC_CTYPE='{collate}'"
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def drop_table(self, table_name):
        """删除表"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        sql = f"DROP TABLE IF EXISTS {table_name};"
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def show_table(self):
        """显示数据库中所有表名"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        cursor.execute(sql)
        row = cursor.fetchall()
        connect.commit()
        cursor.close()
        connect.close()
        return row

    def show_table_by_database_name(self, name: str):
        """显示指定数据库的表"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        sql = f"SELECT table_name AS '表名' FROM information_schema.tables WHERE table_schema = '{name}';"
        cursor.execute(sql)
        row = cursor.fetchall()
        connect.commit()
        cursor.close()
        connect.close()
        return row

    def show_database(self):
        """显示所有数据库"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        sql = f"SELECT datname FROM pg_database;"
        cursor.execute(sql)
        row = cursor.fetchall()
        connect.commit()
        cursor.close()
        connect.close()
        return row

    def drop_database(self, database_name):
        """删除数据库"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        sql = f"DROP DATABASE IF EXISTS {database_name}"
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def alter_table_name(self, table_name, new_table_name):
        """更改表名"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        sql = f"ALTER TABLE {table_name} RENAME TO {new_table_name}"
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def drop_column(self, table_name, column):
        """删除表中的某个字段"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        sql = f"ALTER TABLE {table_name} DROP COLUMN {column};"
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def alter_column_type(self, table_name: str, column_name: str, column_type: str, length: int = None,
                          is_not_null: bool = True, is_primary_key: bool = False,
                          is_auto_increment: bool = False):
        """更改表中某字段的类型"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        constraint = ""
        if is_not_null:
            constraint += " NOT NULL"
        if is_primary_key:
            constraint += " PRIMARY KEY"

        type_def = column_type
        if length is not None and column_type not in ["text", "boolean", "integer", "bigint", "smallint"]:
            type_def = f"{column_type}({length})"

        sql = f"ALTER TABLE {table_name} ALTER COLUMN \"{column_name}\" TYPE {type_def}"
        if constraint:
            sql += f" {constraint}"

        # 处理自增（PostgreSQL使用SERIAL或序列）
        if is_auto_increment and column_type.lower() in ["integer", "bigint", "smallint"]:
            sequence_name = f"{table_name}_{column_name}_seq"
            sql += f"; CREATE SEQUENCE {sequence_name} START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;"
            sql += f" ALTER TABLE {table_name} ALTER COLUMN \"{column_name}\" SET DEFAULT nextval('{sequence_name}'::regclass);"

        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def alter_column_name(self, table_name: str, column_name: str, new_column_name: str,
                          column_type: str, length: int = None):
        """更改表中某字段的名字"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()

        type_def = column_type
        if length is not None and column_type not in ["text", "boolean", "integer", "bigint", "smallint"]:
            type_def = f"{column_type}({length})"

        sql = f"ALTER TABLE {table_name} RENAME COLUMN \"{column_name}\" TO \"{new_column_name}\";"
        sql += f" ALTER TABLE {table_name} ALTER COLUMN \"{new_column_name}\" TYPE {type_def};"

        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def add_column(self, table_name: str, column_name: str, column_type: str = "varchar", length: int = 255,
                   is_not_null: bool = True, is_primary_key: bool = False,
                   is_auto_increment: bool = False, is_first: bool = False):
        """向表中新增字段"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        constraint = ""
        if is_not_null:
            constraint += " NOT NULL"
        if is_primary_key:
            constraint += " PRIMARY KEY"

        type_def = column_type
        if length is not None and column_type not in ["text", "boolean", "integer", "bigint", "smallint"]:
            type_def = f"{column_type}({length})"

        sql = f"ALTER TABLE {table_name} ADD COLUMN \"{column_name}\" {type_def}{constraint}"

        if is_first:
            # PostgreSQL不直接支持FIRST，需要获取现有列并重建表
            columns = self.show_columns(self.__db__, table_name)
            if columns:
                first_column = columns[0][0]
                sql = f"CREATE TEMP TABLE temp_{table_name} AS SELECT \"{first_column}\", * FROM {table_name};"
                sql += f" DROP TABLE {table_name};"
                sql += f" CREATE TABLE {table_name} LIKE temp_{table_name};"
                sql += f" INSERT INTO {table_name} SELECT * FROM temp_{table_name};"
                sql += f" DROP TABLE temp_{table_name};"
                sql += f" ALTER TABLE {table_name} ADD COLUMN \"{column_name}\" {type_def}{constraint} FIRST;"
            else:
                sql = f"ALTER TABLE {table_name} ADD COLUMN \"{column_name}\" {type_def}{constraint};"

        # 处理自增
        if is_auto_increment and column_type.lower() in ["integer", "bigint", "smallint"]:
            sequence_name = f"{table_name}_{column_name}_seq"
            sql += f"; CREATE SEQUENCE {sequence_name} START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;"
            sql += f" ALTER TABLE {table_name} ALTER COLUMN \"{column_name}\" SET DEFAULT nextval('{sequence_name}'::regclass);"

        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def create_index(self, table_name: str, column_name: str, index_name: str):
        """创建索引"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        sql = f"CREATE INDEX {index_name} ON {table_name} (\"{column_name}\");"
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def create_unique_index(self, table_name: str, column_name: str, index_name: str):
        """创建唯一索引"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        sql = f"CREATE UNIQUE INDEX {index_name} ON {table_name} (\"{column_name}\");"
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def drop_index(self, table_name: str, index_name: str):
        """删除索引"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        sql = f"DROP INDEX {index_name};"
        cursor.execute(sql)
        connect.commit()
        cursor.close()
        connect.close()

    def get_cursor(self):
        """返回游标"""
        connect = self.__pool__.connection()
        cursor = connect.cursor()
        return cursor

    def get_connection(self):
        """返回连接"""
        connect = self.__pool__.connection()
        return connect

    def show_columns(self, database, table: str):
        """获取某张表的所有字段"""
        columns = ["column_name", "data_type"]
        dt = self.select("information_schema.columns", columns) \
            .equal("table_schema", database, "and").equal("table_name", table, "and").run()
        return dt

    def close(self):
        self.__pool__.close()
