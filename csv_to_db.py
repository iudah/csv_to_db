# argument parser
import argparse

# csv module
import csv

# regex module
import re

# import mariadb connector
import mariadb

# CLI hidden password
import getpass


def match_type(pattern, type, field):
    match = re.findall(pattern, field)
    if len(match) == 1 and len(match[0]) == len(field):
        return type
    return None


def guess_sql_type(field):
    if field is None or len(field) == 0:
        return None

    # use regex matching to guess type
    pattern_type_pairs = [
        ("\d{4}[./-]\d{2}[./-]\d{2}", "date"),
        ("\d{2}[./-]\d{2}[./-]\d{4}", "date"),
        ("\d{2}[./-]\d{2}[./-]\d{2}", "date"),
        ("[-+]?\d+[.]\d+", "float(15)"),
        ("[+-]?\d+", "float(15)"),
    ]
    for pattern, type in pattern_type_pairs:
        if type := match_type(pattern, type, field):
            return type
    return "varchar(50)"


def predict_column_types(rows):
    row = next(rows)
    types = [guess_sql_type(field) for field in row]

    no_of_typed_columns = 0
    for type in types:
        if type is not None:
            no_of_typed_columns += 1

    for row in rows:
        if no_of_typed_columns == len(row):
            break
        for i, (field, column_type) in enumerate(zip(row, types)):
            if len(field) != 0 and column_type is None:
                types[i] = guess_sql_type(field)
                no_of_typed_columns += 1
    return types


def get_db_connection(host, port, user, password):
    try:
        conn = mariadb.connect(host=host, port=port, user=user, password=password)
        return conn
    except mariadb.Error as err:
        print(f"Error occurred while connecting MariaDB: {err}")
        return None


def extract_file_name(path):
    return re.sub("[^A-Za-z0-9]", "_", re.split("[\\/.]", path)[-2])


def csv_to_db(path, host, port, user, db, table_name, drop, keep):
    print("")
    print("Loading CSV")
    with open(path) as file:
        csv_reader = csv.reader(file)

        print("Collecting column names and types")
        column_names = next(csv_reader)
        column_types = predict_column_types(csv_reader)

        password = getpass.getpass(prompt="MariaDB password:")

        # connect to db
        conn = get_db_connection(host, port, user, password)
        if conn is None:
            return
        print(f"Connected to MariaDB as `{user}`@`{host}`")
        # ensure database exists
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {db};")
        # create table
        if table_name is None:
            table_name = extract_file_name(path)

        table = f"{db}.{table_name}"
        if drop:
            cur.execute(f"DROP TABLE IF EXISTS {table};")
        table_creation = f"CREATE TABLE {' IF NOT EXISTS ' if keep else ''} {table} ("

        for column, type in zip(column_names, column_types):
            table_creation += f"{column} {type},\n"
        table_creation = table_creation[:-2] + "\n);"

        cur.execute(table_creation)

        insertion = f"INSERT INTO {table} VALUES"
        insertion += re.sub("[']", "", f" {tuple(['?' for _ in column_names])};")

        csv_reader = csv.reader(file)
        next(csv_reader)

        print(f"Inserting CSV data into `{table}`")
        no_of_rows = 0
        for row in csv_reader:
            cur.execute(
                insertion, [(field if len(field) > 0 else None) for field in row]
            )
            no_of_rows += 1
        print(f"Inserted {no_of_rows} records")

        conn.commit()
        conn.close()

    print("CSV exported to MariaDB")


# main entry
def main():
    parser = argparse.ArgumentParser(description="Export CSV to MariaDB")
    parser.add_argument("path", help="path to csv file")
    parser.add_argument(
        "--host", nargs="?", default="localhost", help="MariaDB host to connect to"
    )
    parser.add_argument(
        "--port", nargs="?", default=3306, help="port to connect through", type=int
    )
    parser.add_argument("--user", help="user name of client")
    parser.add_argument("--db_name", help="database name to create table")
    parser.add_argument(
        "--table_name", nargs="?", default=None, help="unique name for table"
    )

    table_option_grp = parser.add_mutually_exclusive_group()
    table_option_grp.add_argument("--drop_exists", action="store_true")
    table_option_grp.add_argument("--keep", action="store_true")

    args = parser.parse_args()

    csv_to_db(
        args.path,
        args.host,
        args.port,
        args.user,
        args.db_name,
        args.table_name,
        args.drop_exists,
        args.keep,
    )
    pass


# entry point
if __name__ == "__main__":
    main()
