# SPDX-License-Identifier: MIT
# Copyright (c) 2023 Akumatic

from json import dumps
from collections import defaultdict
from .utf8mb4converter import UTF8MB4Converter, DEFAULT_CHARSET

class Statistics:
    """
    Class for creating statistics that give an overview of the character set status of the database

    Attributes:
    - dbcon (UTF8MB4Converter)
        - The converter object storing the database information and connection 
    - data (dict)
        - A dictionary holding the generated data: Number of tables & columns and character set overview
    """

    def __init__ (
            self,
            dbcon: UTF8MB4Converter
        ) -> None:
        """
        Constructor of Statistics object. Generates statistics at creation.

        Parameters:
        - dbcon (UTF8MB4Converter)
            - The converter object storing the database information and connection 
        """

        self.dbcon = dbcon
        self.data: dict = None
        self.update_stats()
    
    def __str__ (
            self
        ) -> str:
        """
        String representation method. Prints the data stored if the object is passed to print() or str()

        Returns:
        - A string representation of the data stored in the object by calling get_data_formatted_str
        """

        return self.get_data_formatted_str()
    
    def get_data_formatted_str (
            self,
            indent: int = 4
        ) -> str:
        """
        Formats the stored data for better readability.

        Parameters:
        - indent (int)
            - number of spaces for indentation
            - default value: 4

        Returns:
        - String representation of the data stored in the object
        """

        return dumps(self.data, indent=indent)
    
    def update_stats ( 
            self
        ) -> None:
        """
        Fetches data from the database and stores it in the object.
        """

        tables = self.dbcon.get_tables()

        # store dict with count of tables and columns
        count_tab = len(tables)
        count_col = 0

        # store dict with count of different charsets of tables and columns
        charset_tab = defaultdict(int)
        charset_col = defaultdict(int)

        # fill prepared dicts with data
        for table in tables:
            charset = self.dbcon.get_charset_table(table)["charset"]
            charset_tab[charset] += 1

            columns = self.dbcon.get_columns_of_table(table)
            count_col += len(columns)
            for column in columns:
                charset_col[column["charset"]] += 1
        
        # store generated data into the object itself
        self.data = {
            "count": {
                "tables": count_tab,
                "columns": count_col
            },
            "charset": {
                "tables": charset_tab,
                "columns": charset_col
            },
            "converted": {
                "tables": {
                    "converted": charset_tab[DEFAULT_CHARSET],
                    "missing": count_tab - charset_tab[DEFAULT_CHARSET]
                },
                "columns": {
                    "converted": charset_col[DEFAULT_CHARSET],
                    "missing": count_col - charset_col[DEFAULT_CHARSET] - charset_col[None]
                }
            }
        }