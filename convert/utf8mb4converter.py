# SPDX-License-Identifier: MIT
# Copyright (c) 2023 Akumatic

import mariadb
import logging
import sys

DEFAULT_CHARSET = "utf8mb4"
DEFAULT_COLLATION = "utf8mb4_unicode_520_ci"

class UTF8MB4Converter:
    """
    Class to establish a database connection and execute SQL queries to change the default character set and collation.

    Attributes:
    - db (str)
        - Stores database name of database connection
    - host (str)
        - Stores host of database connection
    - port (int)
        - Stores port of database connection
    - logger (logging.Logger)
        - Logger object for this file
    - _connection (mariadb.Connection)
        - database connection
    - cur (mariadb.Cursor)
        - database cursor
    """

    def __init__(
            self,
            user: str,
            password: str,
            host: str,
            port:int,
            db: str
        ) -> None:
        """
        Constructor of UTF8MB4Converter object. Establishes a connection to the given database and stores the cursor.
        Exits the program if connection fails.
        """
        
        self.db: str = db
        self.host: str = host
        self.port: int = port
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.logger.info(f"Connecting to {db}@{host}:{port} as {user}")
        
        try:
            self._connection: mariadb.Connection = mariadb.connect(
                user = user,
                password = password,
                host = host,
                port = port,
                database = db
            )
            self.logger.info("Connection established")
        except mariadb.Error as e:
            self.logger.fatal(f"Connection failed: {e}")
            sys.exit()

        self.cur: mariadb.Cursor = self._connection.cursor()

    def __del__(
            self
        ) -> None:
        """
        Destructor of UTF8MB4Converter object. Closes the established connection.
        """

        self.logger.info(f"Connection to {self.db}@{self.host}:{self.port} closed")
        self._connection.close()

    def get_tables (
            self
        ) -> list:
        """ 
        Fetches all tables of the given database.

        Returns:
        - List of strings containing names of all tables
        """
        
        query = f"SHOW TABLES FROM {self.db}"
        self.cur.execute(query)
        return [table[0] for table in self.cur.fetchall()]
    
    def get_charset_db (
            self
        ) -> dict:
        """ 
        Fetches the character set and collation of the database.

        Returns:
        - A dict with the keys charset and collation, containing the collation
          and character set of the stored database.
        """

        query = " ".join((
            "SELECT default_character_set_name, default_collation_name",
            f"FROM information_schema.SCHEMATA WHERE schema_name = '{self.db}'"
        ))
        self.cur.execute(query)
        return dict(zip(("charset", "collation"), self.cur.fetchone()))

    def get_charset_table (
            self,
            table: str
        ) -> dict:
        """
        Fetches the character set and collation of a given table

        Parameters:
        - table (str)
            - the table whose character set is to be changed

        Returns: 
        - A dict with the keys "charset" and "collation", containing the
          collation and character set of the given table.
        """

        query = " ".join((
            "SELECT CCSA.character_set_name, table_collation",
            "FROM information_schema.TABLES AS T",
            "JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY AS CCSA", 
            "WHERE T.table_collation = CCSA.collation_name",
            f"AND table_schema = '{self.db}' AND table_name = '{table}'"
        ))
        self.cur.execute(query)
        return dict(zip(("charset", "collation"), self.cur.fetchone()))
    
    def get_columns_of_table (
            self,
            table: str
        ) -> list:
        """
        Fetches information about the columns of a given table.

        Parameters:
        - table (str)
            - the tables whose columns are to be retrieved

        Returns: 
        - A list of dicts, containing the column information. Each dict has
          the keys "name" (Column Name), "type" (Data Type), "ctype" (Column Type),
          "charset", "collation", "nullable" and "dvalue" (Column Default)
        """

        query = " ".join((
            "SELECT COLUMN_NAME AS name, DATA_TYPE AS type, COLUMN_TYPE AS ctype,", 
            "CHARACTER_SET_NAME AS charset, COLLATION_NAME as collation,", 
            "IS_NULLABLE AS nullable, COLUMN_DEFAULT AS dvalue",
            "FROM information_schema.COLUMNS", 
            f"WHERE table_schema = '{self.db}' AND table_name = '{table}'"
        ))
        self.cur.execute(query)
        lables = ["name", "type", "ctype", "charset", "collation", "nullable", "dvalue"]
        return [dict(zip(lables, col)) for col in self.cur.fetchall()]

    def convert_charset_db (
            self,
            charset: str = DEFAULT_CHARSET,
            collation: str = DEFAULT_COLLATION
        ) -> None:
        """
        Alters the charset and collation of the database

        Parameters:
        - charset (str)
            - target character set
            - default value: utf8mb4
        - collation (str)
            - target collation
            - default value: utf8mb4_unicode_520_ci
        """

        self.logger.debug(f"Start converting character set of database {self.db} to {charset}")
        db_info = self.get_charset_db()

        if db_info["charset"] == charset:
            self.logger.debug(f"Database {self.db} already has character set {charset}")
            return
        
        query = f"ALTER DATABASE {self.db} CHARACTER SET = {charset} COLLATE = {collation}"
        try:
            self.cur.execute(query)
            self.logger.debug(f"Character set of database {self.db} successfully converted to {charset}")
        except mariadb.Error as e:
            self.logger.error("\n".join((
                f"Failed to convert character set of database {self.db}: {e}",
                f"-> Query causing the problem: {query}"
            )))

    def convert_charset_single_table (
            self,
            table: str,
            charset: str = DEFAULT_CHARSET,
            collation: str = DEFAULT_COLLATION
        ) -> None:
        """
        Alters the charset and collation of a single table

        Parameters:
        - table (str)
            - the table to be altered
        - charset (str)
            - target character set
            - default value: utf8mb4
        - collation (str)
            - target collation
            - default value: utf8mb4_unicode_520_ci
        """

        self.logger.debug(f"Start converting character set of table {table} to {charset}")
        table_info = self.get_charset_table(table)

        if table_info["charset"] == charset:
            self.logger.debug(f"Table {table} already has character set {charset}")
            return
        
        query = f"ALTER TABLE {table} CONVERT TO CHARACTER SET {charset} COLLATE {collation}"
        try:
            self.cur.execute(query)
            self.logger.debug(f"Character set of table {table} successfully converted to {charset}")
        except mariadb.Error as e:
            self.logger.error("\n".join((
                f"Failed to convert character set of table {table}: {e}",
                f"-> Query causing the problem: {query}"
            )))

    def convert_charset_all_tables (
            self,
            charset: str = DEFAULT_CHARSET,
            collation: str = DEFAULT_COLLATION
        ) -> None:
        """
        Alters the charset and collation of all tables of the database.

        Parameters:
        - charset (str)
            - target character set
            - default value: utf8mb4
        - collation (str)
            - target collation
            - default value: utf8mb4_unicode_520_ci
        """

        tables = self.get_tables()
        for table in tables:
            self.convert_charset_single_table(table, charset, collation)

    def convert_charset_single_column (
            self,
            column: dict,
            table: str,
            charset: str = DEFAULT_CHARSET,
            collation: str = DEFAULT_COLLATION
        ) -> None:
        """
        Alters the charset and collation of a single column.

        Parameters:
        - column (dict)
            - a dict containig information about the column to be altered
        - table (str)
            - the table housing the column
        - charset (str)
            - target character set
            - default value: utf8mb4
        - collation (str)
            - target collation
            - default value: utf8mb4_unicode_520_ci
        """

        col = column["name"]
        self.logger.debug(f"Start converting character set of column {col}(@{table}) to {charset}")

        if not column["charset"]:
            self.logger.debug(f"Column {col}(@{table}) has no default character set")
            return
        if not column["type"] in ["char", "varchar", "text", "longtext"]:
            self.logger.debug(f"Column {col}(@{table}) does contain data of type {column['type']}")
            return
        if column["charset"] == charset:
            self.logger.debug(f"Column {col}(@{table}) already has character set {charset}")
            return
        
        query = " ".join((
            f"ALTER TABLE {table} CHANGE {col} {col}",
            f"{column['ctype']} CHARACTER SET {charset} COLLATE {collation}",
            "NULL" if column["nullable"] == "YES" else f"NOT NULL DEFAULT {column['dvalue']}"
        ))
        try:
            self.cur.execute(query)
            self.logger.debug(f"Character set of column {col}(@{table}) successfully converted to {charset}")
        except mariadb.Error as e:
            self.logger.error("\n".join((
                f"Failed to convert character set of column {col}(@{table}): {e}",
                f"-> Query causing the problem: {query}"
            )))

    def convert_charset_all_columns_single_table (
            self,
            table: str,
            charset: str = DEFAULT_CHARSET,
            collation: str = DEFAULT_COLLATION
        ) -> None:
        """
        Alters the charset and collation of all columns of a table.

        Parameters:
        - table (str)
            - the table containing the columns
        - charset (str)
            - target character set
            - default value: utf8mb4
        - collation (str)
            - target collation
            - default value: utf8mb4_unicode_520_ci
        """

        columns = self.get_columns_of_table(table)
        for column in columns:
            self.convert_charset_single_column(column, table, charset, collation)

    def convert_charset_all_columns_all_tables (
            self,
            charset: str = DEFAULT_CHARSET,
            collation: str = DEFAULT_COLLATION
        ) -> None:        
        """
        Alters the charset and collation of all columns of all tables of a database.

        Parameters:
        - charset (str)
            - target character set
            - default value: utf8mb4
        - collation (str)
            - target collation
            - default value: utf8mb4_unicode_520_ci
        """

        tables = self.get_tables()
        for table in tables:
            self.convert_charset_all_columns_single_table(table, charset, collation)