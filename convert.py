# SPDX-License-Identifier: MIT
# Copyright (c) 2023 Akumatic

import logging
import argparse
from json import dumps
from convert.validation import Validation
from convert.statistics import Statistics
from convert.utf8mb4converter import UTF8MB4Converter, DEFAULT_CHARSET

def main (
        args: argparse.Namespace
    ) -> None:
    """ 
    Main program sequence. Establishes a connection to the database and either creates statistics 
    or converts the database itself, all tables and all text fields to utf8mb4 if they don't already
    have this character set.

    Parameters:
    - args (argparse.Namespace)
        - Contains arguments passed to the program
    """
    logger: logging.Logger = logging.getLogger("Main")
    db: UTF8MB4Converter = UTF8MB4Converter (
        user = args.user,
        password = args.password,
        host = args.host,
        port = args.port,
        db = args.database
    )

    if args.statistics:
        stats: Statistics = Statistics(db)
        logger.info(f"Database statistics:\n{stats}")

    elif args.validate:
        validator = Validation(db)
        validation: dict = validator.convert_validate()
        logger.info(f"Database conversion validation:\n{dumps(validation, indent=4)}")

    else:
        db.convert_charset_all()

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
    args_exc: argparse._MutuallyExclusiveGroup = argparser.add_mutually_exclusive_group()

    args_opt.add_argument("-v", "--verbose", action="store_true")

    args_exc.add_argument("-s", "--statistics", action="store_true")
    args_exc.add_argument("-V", "--validate", action="store_true")

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