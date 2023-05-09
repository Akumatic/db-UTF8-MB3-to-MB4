# db-UTF8-MB3-to-MB4

MySQL has used utf8 as an alias for utf8mb3 in previous versions. Since only three bytes are used per character, not the entire Unicode character set is covered. Also, utf8mb3 has been deprecated since MySQL Version 8.0 and will be removed in future versions.

To convert an existing database, the character sets of the following components must be changed:

- the database
- all tables
- all columns with a character set

This script converts the character set of a given database - by default to **utf8mb4** with collation **utf8mb4_unicode_520_ci**.

If an error occurs during the conversion of a table or column, an output with the corresponding SQL command is issued and the program continues. 

## Usage

```
python convert.py [-h] [-v] [-s | -V] -H HOST -P PORT -u USER -p PASSWORD -d DATABASE
```

Options:
- `-h/--help`
- `-s/--statistics`
- `-V/--validate`

Required arguments:
- `-H/--host HOST`
- `-P/--port PORT`
- `-u/--user USER`
- `-p/--password PASSWORD`
- `-d/--database DATABASE`

Optional arguments:
- `-v/--verbose`
