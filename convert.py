# SPDX-License-Identifier: MIT
# Copyright (c) 2023 Akumatic

import logging
import argparse
from convert.statistics import Statistics
from convert.utf8mb4converter import UTF8MB4Converter

def main (
        args: argparse.Namespace
    ) -> None:
    """ 
    Main program sequence. Establishes a connection to the database and either creates statistics 
    or converts the database itself, all tables and all text fields to utf8mb4 if they don't already
    have this character set.

    Params:
    - args (argparse.Namespace)
        - Contains arguments passed to the program
    """
    
    db: UTF8MB4Converter = UTF8MB4Converter (
        user = args.user,
        password = args.password,
        host = args.host,
        port = args.port,
        db = args.database
    )

    if args.statistics:
        stats = Statistics(db)
        logging.getLogger("Main").info(f"Database statistics:\n{stats}")
    else:
        db.convert_charset_db()
        db.convert_charset_all_tables()
        db.convert_charset_all_columns_all_tables()

def parse_args (
    ) -> argparse.Namespace:
    """
    Parses the arguments passed to the program.

    Returns:
        - An argparse namespace containing the parsed arguments
    """

    argparser: argparse.ArgumentParser = argparse.ArgumentParser()  
    args_opt: argparse._ArgumentGroup = argparser.add_argument_group("Optional Arguments")
    args_req: argparse._ArgumentGroup = argparser.add_argument_group("Required Arguments")

    args_opt.add_argument("-v", "--verbose", action="store_true")
    args_opt.add_argument("-s", "--statistics", action="store_true")

    args_req.add_argument("-H", "--host", required=True)
    args_req.add_argument("-P", "--port", required=True, type=int)
    args_req.add_argument("-u", "--user", required=True)
    args_req.add_argument("-p", "--password", required=True)
    args_req.add_argument("-d", "--database", required=True)
    
    return argparser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO))
    main(args)