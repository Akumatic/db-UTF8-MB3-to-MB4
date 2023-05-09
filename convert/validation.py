# SPDX-License-Identifier: MIT
# Copyright (c) 2023 Akumatic

from collections import defaultdict
from convert.utf8mb4converter import UTF8MB4Converter

class MissingStateException(Exception):
    """
    Custom exception indicating a missing state from validation object.
    """

class Validation:
    """
    Class for validating the conversion of the database. The state of the database 
    before and after conversion is queried and compared. Deviations are shown.

    Attributes:
    - dbcon (UTF8MB4Converter)
        - The converter object storing the database information and connection 
    - start (defaultdict)
        - A dictionary holding the state of the database, before conversion.
    - end (defaultdict)
        - A dictionary holding the state of the database, after conversion.
    """

    def __init__ (
            self,
            dbcon: UTF8MB4Converter
        ) -> None:
        """
        Constructor of Validation object. Generates overview of the database at creation.

        Parameters:
        - dbcon (UTF8MB4Converter)
            - The converter object storing the database information and connection 
        """

        self.dbcon: UTF8MB4Converter = dbcon
        self.start: defaultdict = None
        self.end: defaultdict = None

    def generate_start_state (
            self
        ) -> None:
        """
        Fetches current column schema of the database and stores it in the start attribute.
        """

        self.start = self._get_state()

    def generate_end_state (
            self
        ) -> None:
        """
        Fetches current column schema of the database and stores it in the end attribute.
        """

        self.end = self._get_state()

    def compare_states (
            self
        ) -> dict:
        """
        Compares start and end state and stores information about changed schemas.
        generate_start_state and generate_end_state should be called first.

        Returns:
        - A dict containing a numeric summary and details about mismatched columns.

        Raises:
        - MissingStateException
            - Raised when either start state or end state is not set
        """

        if self.start is None:
            raise MissingStateException("No start state stored. Make sure to call generate_start_state")
        if self.end is None:
            raise MissingStateException("No end state stored. Make sure to call generate_end_state")

        summary: dict = {"unaltered": 0, "altered": 0}
        details: defaultdict = defaultdict(dict)

        for table in self.start.keys():
            a: dict = self.start[table]
            b: dict = self.end[table]
            for column in a.keys():
                comp: dict = self._get_differences(a[column], b[column])
                if len(comp) == 0:
                    summary["unaltered"] += 1
                else:
                    summary["altered"] += 1
                    details[table][column] = comp

        return {"summary": summary, "details": details}
    
    def convert_validate (
            self
        ) -> dict:
        """
        Alters the charset and collation of the database, all columns and all tables.
        Validates that no other field was changed.

        Returns:
        - A dict containing a numeric summary and details about mismatched columns.
        """

        self.generate_start_state()
        self.dbcon.convert_charset_all()
        self.generate_end_state()
        return self.compare_states()

    def _get_differences (
            self,
            a: dict,
            b: dict
        ) -> dict:
        """
        Compares two given column data sets and compares the values for all keys
        but for the fields changed by character set conversion (CHARACTER_SET_NAME, 
        COLLATION_NAME and CHARACTER_OCTET_LENGTH). Stores before and after value
        for each column.

        Parameters:
        - a (dict)
            - A dictionary containing the information schema of a column.
        - b (dict)
            - A dictionary containing the information schema of a column
            - Used for comparison with dictionary a

        Returns:
        - A dict with the keys and values of deviations between the two given dicts,
          ignoring certain values changed by character set conversion.
        """

        data = dict()
        keys = a.keys()
        for key in keys:
            if key == "CHARACTER_SET_NAME":
                continue
            if key == "COLLATION_NAME":
                continue
            if key == "CHARACTER_OCTET_LENGTH":
                continue
            if a[key] != b[key]:
                data[key] = {
                    "Before": a[key],
                    "After": b[key]
                }
        return data
    
    def _get_columns_of_table (
            self,
            table: str
        ) -> list:
        """
        Fetches all information about the columns of a given table.

        Parameters: 
        - table (str)
            - the tabke whose columns are to be retrieved

        Returns:
        - A list of dicts, containing the full column information.
        """

        query = " ".join((
            "SELECT * FROM information_schema.COLUMNS", 
            f"WHERE table_schema = '{self.dbcon.db}' AND table_name = '{table}'"
        ))
        self.dbcon.kcursor.execute(query)
        return self.dbcon.kcursor.fetchall()
    
    def _get_state (
            self
        ) -> defaultdict:
        """
        Fetches column schema of the database and stores it for each table and column.

        Returns:
        - A defaultdict that contains one dictionary for each table.
          Each table dictionary contains one dict per column, which contains the column schema.
        """

        state: defaultdict = defaultdict(dict)
        tables: list = self.dbcon.get_tables()
        for table in tables:
            columns: list = self._get_columns_of_table(table)
            for column in columns:
                state[table][column["COLUMN_NAME"]] = column
        return state