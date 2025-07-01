from babySql.class_methods import MariaDB
from babySql.class_methods import MySQL
from babySql.class_methods import SqLite
from babySql.class_methods import PostgreSQL


class BabySql:
    def __init__(self, dt_type, host=None, port=None, user=None, password=None, database=None, max_connections=50):
        """
        统一数据库访问接口
        :param dt_type: 数据库类型 ("MySQL", "SqLite", "MariaDB")
        :param host: 数据库主机地址 (MySQL/MariaDB 需要)
        :param port: 数据库端口 (MySQL/MariaDB 需要)
        :param user: 数据库用户名 (MySQL/MariaDB 需要)
        :param password: 数据库密码 (MySQL/MariaDB 需要)
        :param database: 数据库名称或SQLite文件路径
        :param max_connections: 连接池最大连接数
        """
        self.__dt_type__ = dt_type.lower()
        self.__host__ = host
        self.__port__ = port
        self.__user__ = user
        self.__password__ = password
        self.__database__ = database
        self.__max_connections__ = max_connections
        # 根据数据库类型创建连接
        self.__connection = self._create_connection()

    def _create_connection(self):
        """创建数据库连接对象"""
        if self.__dt_type__ == "mysql":
            return MySQL(
                host=self.__host__,
                port=self.__port__,
                user=self.__user__,
                passwd=self.__password__,
                db=self.__database__,
                max_connections=self.__max_connections__
            )
        elif self.__dt_type__ == "mariadb":
            return MariaDB(
                host=self.__host__,
                port=self.__port__,
                user=self.__user__,
                passwd=self.__password__,
                db=self.__database__,
                max_connections=self.__max_connections__
            )
        elif self.__dt_type__ == "postgresql":
            return PostgreSQL(
                host=self.__host__,
                port=self.__port__,
                user=self.__user__,
                passwd=self.__password__,
                db=self.__database__,
                max_connections=self.__max_connections__
            )
        elif self.__dt_type__ == "sqlite":
            return SqLite(
                database=self.__database__,
                max_connections=self.__max_connections__
            )
        else:
            raise ValueError(f"不支持的数据库类型: {self.__dt_type__}")

    @property
    def connect(self):
        """获取数据库连接对象"""
        return self.__connection

    def get_connection(self):
        """获取数据库连接对象（兼容方法）"""
        return self.__connection

    def close(self):
        """关闭所有数据库连接"""
        if self.__connection:
            self.__connection.close()

    def __enter__(self):
        """支持上下文管理器"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文时自动关闭连接"""
        self.close()
