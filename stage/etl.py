from dotenv import dotenv_values
import psycopg2
from os import listdir, rename as file_rename
from os.path import isfile, join
import os
import csv
from psycopg2.extras import execute_values

os.chdir(os.path.dirname(os.path.realpath(__file__)))
CSV_FILES_PATH_NEW = 'source_data/new'
CSV_FILES_PATH_PROCESSED = 'source_data/processed'
CSV_FILES_PATH_ERROR = 'source_data/error'

target_table = dotenv_values().get("STAGE_SALES_SOURCE_TABLE")
meta_table = dotenv_values().get("STAGE_POSTGRES_DB_META_TABLE")


class ValidationDataError(Exception):
    pass


class InsertMetaError(Exception):
    pass


class UpdateMetaError(Exception):
    pass


class InsertDataError(Exception):
    pass


def prepare_csv_line_to_insert(inserting_line):
    from datetime import datetime
    val_count = 6
    types = {
        0: lambda val: int(val),
        1: lambda val: str(val),
        2: lambda val: int(val),
        3: lambda val: float(val),
        4: lambda val: datetime.strptime(val, '%m/%d/%y %H:%M'),
        5: lambda val: str(val),
    }

    return [types[i](inserting_line[i]) for i in range(val_count)]



def insert_data(connection: psycopg2._psycopg.connection, target_table, data):
    try:
        with connection:
            with connection.cursor() as cur:
                execute_values(
                    cur,
                    f"INSERT INTO {target_table} ("
                    f"order_id, product, quantity_ordered, price_per_each, "
                    f"order_date, purchase_address, etl_meta_info_id) "
                    f"VALUES %s",
                    data
                )
    except psycopg2.Error as error:
        raise InsertDataError(error.diag.message_primary)
    except Exception as error:
        raise InsertDataError(error)


def insert_meta(connection: psycopg2._psycopg.connection, source: str, meta_table: str, target_table: str):
    try:
        with connection:
            with connection.cursor() as cur:
                cur.execute(f"INSERT INTO {meta_table} (end_date, state, source, target_table, log) "
                            f"VALUES (NULL, 'processing', '{source}', '{target_table}', 'Starting processing.') "
                            f"RETURNING id")
                cur.connection.commit()
                return cur.fetchone()[0]
    except psycopg2.Error as error:
        raise InsertMetaError(f"Error while inserting metainfo: {error.diag.message_primary}. ETL was terminated.")
    except Exception as error:
        raise InsertMetaError(f"Error while inserting metainfo: {error}. ETL was terminated.")


def updated_meta_info(
        connection: psycopg2._psycopg.connection,
        meta_table: str,
        etl_meta_info_id: int,
        status=None, appendix_message=None, line_number=None):
    sql_statement = f"UPDATE {meta_table} SET \n"
    try:
        # Define cursor for existing connection
        with connection:
            with connection.cursor() as cur:
                # Get current 'log' value of meta info.
                cur.execute(f"SELECT log FROM {meta_table} WHERE id = {etl_meta_info_id}")
                log = cur.fetchone()[0]
                # Update 'log' depends on status
                match status:
                    case 'success':
                        sql_statement += "end_date = now(), state = 'finished', \n"
                        log += "\n\nSUCCESS: Loading was finished."
                    case 'warning':
                        log += f"\n\nWARNING, CSV LINE {line_number}: {appendix_message}"
                    case 'error':
                        sql_statement += "end_date = now(), state = 'error', \n"
                        log += f"\n\nERROR: {appendix_message}"
                    case _:
                        raise Exception(f"updating status '{status}'cannot be processed.")
                # Appending log and where clause in sql statement for proper etl_meta_info_id
                sql_statement += f"log = '{log}' \nWHERE id = {etl_meta_info_id};"
                cur.execute(sql_statement)
    # Exception handling
    except psycopg2.Error as error:
        raise UpdateMetaError(f"Error while updating metainfo: {error.diag.message_primary}")
    except Exception as error:
        raise UpdateMetaError(f"Error while updating metainfo: {error}")


if __name__ == '__main__':
    try:
        # Connection to target db
        connection = psycopg2.connect(
            host=dotenv_values().get("STAGE_POSTGRES_HOST"),
            port=dotenv_values().get("STAGE_POSTGRES_PORT"),
            database=dotenv_values().get("STAGE_POSTGRES_DB"),
            user=dotenv_values().get("STAGE_POSTGRES_USER"),
            password=dotenv_values().get("STAGE_POSTGRES_PASS")
        )
        # Looking for new csv files to ETL
        files = [f for f in listdir(CSV_FILES_PATH_NEW) if isfile(join(CSV_FILES_PATH_NEW, f))]
        # Starting ETL for each file
        for file in files:
            # Create me
            try:
                etl_meta_info_id = insert_meta(
                    connection=connection,
                    source=f"{CSV_FILES_PATH_NEW}/{file}",
                    meta_table=meta_table,
                    target_table=target_table
                )
            except InsertMetaError as e:
                print(f"{e}. Stopping ETL for file {file}")
                file_rename(f"{CSV_FILES_PATH_NEW}/{file}", f"{CSV_FILES_PATH_ERROR}/{file}")
                continue

            try:
                with open(f"{CSV_FILES_PATH_NEW}/{file}", newline='') as f:
                    reader = csv.reader(f, delimiter=',')
                    header_line = next(reader, None)
                    lines_count = 0
                    data_to_insert = []
                    for line in reader:
                        lines_count += 1
                        try:
                            if line == header_line:
                                raise ValidationDataError("this line is header line")
                            elif '' in line:
                                raise ValidationDataError("empty values in line")
                            inserting_line = prepare_csv_line_to_insert(line)
                            inserting_line.append(int(etl_meta_info_id))
                            if inserting_line in data_to_insert:
                                raise ValidationDataError("duplicate line")
                            data_to_insert.append(tuple(inserting_line))
                        except ValidationDataError as error:
                            updated_meta_info(
                                connection, meta_table, etl_meta_info_id,
                                'warning', error, lines_count)
                    insert_data(connection, target_table, data_to_insert)
                    updated_meta_info(
                        connection, meta_table, etl_meta_info_id,
                        'success')
                file_rename(f"{CSV_FILES_PATH_NEW}/{file}", f"{CSV_FILES_PATH_PROCESSED}/{file}")
            except psycopg2.Error as error:
                print(error.diag.message_primary)
                updated_meta_info(
                    connection, meta_table, etl_meta_info_id,
                    'error', error.pgerror)
            except Exception as error:
                print(error)
                updated_meta_info(
                    connection, meta_table, etl_meta_info_id,
                    'error', error)
    except (Exception, psycopg2.Error) as error:
        print(error)
