from dotenv import dotenv_values
import psycopg2
from ddl_func import create_table_ddl
from stage.db_structure import table_ddl
from psycopg2.extras import execute_values
import datetime

stage_source_table_name = dotenv_values().get("STAGE_SALES_SOURCE_TABLE")
stage_source_table_ddl = table_ddl.get(stage_source_table_name)

class EtlError(Exception):
    pass
class StartEtlError(Exception):
    pass
class InsertEtlMetaError(Exception):
    pass
class UpdateEtlMetaError(Exception):
    pass


def conn_inf(connection: psycopg2._psycopg.connection):
    return \
        f"TYPE: POSTGRES\n" \
        f"HOST: {connection.info.host}\n" \
        f"PORT: {connection.info.port}\n" \
        f"DB: {connection.info.dbname}"


def insert_meta(
        connection: psycopg2._psycopg.connection,
        source: str,
        target: str,
        meta_table: str,
        datetime: datetime.datetime
):
    try:
        with connection.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {meta_table} (end_date, state, source, target, log)
                VALUES (NULL, 'processing', '{source}', '{target}', '[{datetime}] ETL starts...')
                RETURNING id
                """
            )
            meta_info_id = cur.fetchone()[0]
        connection.commit()
        return meta_info_id
    except psycopg2.Error as error:  # error.diag.message_primary
        raise InsertEtlMetaError(f"While inserting etl meta error occured: {error.diag.message_primary}")


def update_meta_info(
        connection: psycopg2._psycopg.connection,
        meta_table: str,
        etl_meta_info_id: int,
        datetime: datetime.datetime,
        etl_log_message: str,
        is_finished=False,
        is_error=False):
    try:
        with connection.cursor() as cur:
            sql_statement = f"UPDATE {meta_table} SET \n"
            # Get current 'log' value of meta info.
            cur.execute(f"SELECT log FROM {meta_table} WHERE id = {etl_meta_info_id}")
            log = f"{cur.fetchone()[0]}\n[{datetime}] {etl_log_message}"
            # Update 'log' depends on status
            if is_finished:
                sql_statement += "end_date = now(), state = 'finished', \n"
            if is_error:
                sql_statement += "end_date = now(), state = 'error', \n"
            # Appending log and where clause in sql statement for proper etl_meta_info_id
            sql_statement += f"log = '{log}' \nWHERE id = {etl_meta_info_id};"
            cur.execute(sql_statement)
        connection.commit()
    except psycopg2.Error as error:  # error.diag.message_primary
        raise UpdateEtlMetaError(f"While updating etl meta error occured: {error.diag.message_primary}")


stage_ods_dim_map = {
        # ODS and Stage dim names mapping. Using info from scripts db_structure.py of ods and stage dirs
        'sales_date': {'ods': 'ts', 'stage': 'order_date', 'fact_id_col': 'datetime_id'},
        'sales_product': {'ods': 'name', 'stage': 'product', 'fact_id_col': 'product_id'},
        'sales_purchase_address': {'ods': 'address', 'stage': 'purchase_address',
                                   'fact_id_col': 'purchase_address_id'},
        'sales_order': {'ods': 'order_id', 'stage': 'order_id', 'fact_id_col': 'order_id'}
    }
temp = [(f'{dim_table}.id', stage_ods_dim_map[dim_table]['fact_id_col']) for dim_table in
                                stage_ods_dim_map]
dim_ids = ", ".join([item[0] for item in temp])
fact_col_ids = ", ".join([item[1] for item in temp])
dim_ids_aliases = ", \n".join([f"{item[0]} {item[1]}" for item in temp])


if __name__ == '__main__':
    try:
        print("Connecting...")
        try:
            stage_connection = psycopg2.connect(
                host=dotenv_values().get("STAGE_POSTGRES_HOST"),
                port=dotenv_values().get("STAGE_POSTGRES_PORT"),
                database=dotenv_values().get("STAGE_POSTGRES_DB"),
                user=dotenv_values().get("STAGE_POSTGRES_USER"),
                password=dotenv_values().get("STAGE_POSTGRES_PASS")
            )
            print("Stage connected sucessfully.")
        except psycopg2.Error as error:
            raise StartEtlError(f"While stage connecting error occurred: \n{error}\n")

        try:
            ods_connection = psycopg2.connect(
                host=dotenv_values().get("ODS_POSTGRES_HOST"),
                port=dotenv_values().get("ODS_POSTGRES_PORT"),
                database=dotenv_values().get("ODS_POSTGRES_DB"),
                user=dotenv_values().get("ODS_POSTGRES_USER"),
                password=dotenv_values().get("ODS_POSTGRES_PASS")
            )
            ods_connection_meta = psycopg2.connect(
                host=dotenv_values().get("ODS_POSTGRES_HOST"),
                port=dotenv_values().get("ODS_POSTGRES_PORT"),
                database=dotenv_values().get("ODS_POSTGRES_DB"),
                user=dotenv_values().get("ODS_POSTGRES_USER"),
                password=dotenv_values().get("ODS_POSTGRES_PASS")
            )
            print("ODS connected sucessfully.")
        except psycopg2.Error as error:  # error.diag.message_primary
            raise StartEtlError(f"While ODS connecting error occured: {error.diag.message_primary}")

        # ETLing new source records
        with ods_connection_meta:
            try:
                sys_etl_meta_info_id = insert_meta(
                    ods_connection_meta, conn_inf(stage_connection), conn_inf(ods_connection),
                    'etl_meta_info', datetime.datetime.now(datetime.timezone.utc)
                )

                with stage_connection, ods_connection:
                    with stage_connection.cursor() as stage_cur, ods_connection.cursor() as ods_cur:
                        update_meta_info(
                            ods_connection_meta, 'etl_meta_info', sys_etl_meta_info_id,
                            datetime.datetime.now(datetime.timezone.utc),
                            "Selecting new source data..."
                        )

                        ods_cur.execute("SELECT max(source_created_at) FROM sales_info")
                        max_source_created_at = ods_cur.fetchall()[0][0]

                        if max_source_created_at:
                            stage_cur.execute(f"SELECT * FROM {stage_source_table_name} WHERE created_at > %s",
                                              (max_source_created_at,))
                        else:
                            stage_cur.execute(f"SELECT * FROM {stage_source_table_name}")


                        if stage_cur.rowcount != 0:
                            update_meta_info(
                                ods_connection_meta, 'etl_meta_info', sys_etl_meta_info_id,
                                datetime.datetime.now(datetime.timezone.utc),
                                f"Selecting new source data finished: collected {stage_cur.rowcount} rows. "
                                f"Trying to create temp table in target for source data."
                            )

                            # Creating temp table like source. It's used to insert new values using PostgreSQL, not python
                            ods_cur.execute(create_table_ddl(stage_source_table_name, stage_source_table_ddl, is_temp=True))
                            while True:
                                rows = stage_cur.fetchmany(50000)
                                if not rows:
                                    break
                                execute_values(ods_cur, f"INSERT INTO {stage_source_table_name} VALUES %s", rows)

                            update_meta_info(
                                ods_connection_meta, 'etl_meta_info', sys_etl_meta_info_id,
                                datetime.datetime.now(datetime.timezone.utc),
                                "Temp table in target for source data was created succesfully. "
                                "Target dimension tables updating by new values..."
                            )

                            # Starting ODS dimension tables updating by new values.
                            for ods_table in stage_ods_dim_map:
                                # Selecting values from stage, which are not represented in ods dim tables
                                sqltemp = f"""
                                    SELECT stage.dim
                                    FROM (
                                        SELECT DISTINCT {stage_ods_dim_map[ods_table]['stage']} as dim 
                                        FROM {stage_source_table_name}
                                    ) stage
                                    LEFT JOIN (
                                        SELECT DISTINCT {stage_ods_dim_map[ods_table]['ods']} as dim 
                                        FROM {ods_table}
                                    ) ods 
                                    USING (dim)
                                    WHERE ods.dim IS NULL
                                """
                                # Inserting new values in ODS dim tables
                                ods_cur.execute(sqltemp)
                                execute_values(
                                    ods_cur,
                                    f"INSERT INTO {ods_table} ({stage_ods_dim_map[ods_table]['ods']}{', year, month, day' if ods_table == 'sales_date' else ''}) VALUES %s",
                                    [(val[0], val[0].year, val[0].month, val[0].day) for val in ods_cur.fetchall()] if ods_table == 'sales_date'
                                    else ods_cur.fetchall()
                                )

                            update_meta_info(
                                ods_connection_meta, 'etl_meta_info', sys_etl_meta_info_id,
                                datetime.datetime.now(datetime.timezone.utc),
                                "Target dimension tables were updated successfully. Inserting new values in target fact table..."
                            )

                            # Inserting new values in ODS fact table.
                            joins = " \n".join(
                                [f'JOIN {item} ON {item}.{stage_ods_dim_map[item]["ods"]} = t.{stage_ods_dim_map[item]["stage"]}' for
                                 item in stage_ods_dim_map])
                            ods_cur.execute(f"""
                                INSERT INTO sales_info (
                                    {fact_col_ids}, quantity, price_per_each, total_price,
                                    source_id, source_created_at, source_updated_at,
                                    sys_etl_meta_info_id
                                )
                                SELECT DISTINCT
                                    {dim_ids},
                                    quantity_ordered,
                                    price_per_each,
                                    price_per_each * quantity_ordered,
                                    t.id,
                                    t.created_at,
                                    t.updated_at,
                                    {sys_etl_meta_info_id}
                                FROM {stage_source_table_name} as t 
                                {joins}
                            """)
                            count_insert = ods_cur.rowcount
                            update_meta_info(
                                ods_connection_meta, 'etl_meta_info', sys_etl_meta_info_id,
                                datetime.datetime.now(datetime.timezone.utc),
                                f"Inserting new values in target fact table finished: {count_insert} new facts."
                            )
                        else:
                            update_meta_info(
                                ods_connection_meta, 'etl_meta_info', sys_etl_meta_info_id,
                                datetime.datetime.now(datetime.timezone.utc),
                                "No new data."
                            )
            except (InsertEtlMetaError, UpdateEtlMetaError) as e:
                raise e
            except Exception as e:
                update_meta_info(
                    ods_connection_meta, 'etl_meta_info', sys_etl_meta_info_id,
                    datetime.datetime.now(datetime.timezone.utc),
                    f"Error while trying to ETL new data:{e}",
                    is_error=True
                )
                raise EtlError

            try:
                update_meta_info(
                    ods_connection_meta, 'etl_meta_info', sys_etl_meta_info_id,
                    datetime.datetime.now(datetime.timezone.utc),
                    "Selecting source data to be updated..."
                )

                # ETL: update old records
                with stage_connection, ods_connection:
                    with stage_connection.cursor() as stage_cur, ods_connection.cursor() as ods_cur:
                        ods_cur.execute("SELECT max(source_updated_at) FROM sales_info")
                        max_source_updated_at = ods_cur.fetchall()[0][0]

                        if max_source_updated_at:
                            stage_cur.execute(f"SELECT * FROM {stage_source_table_name} WHERE updated_at > %s",
                                              (max_source_updated_at,))

                        if stage_cur.rowcount not in (0, -1):
                            update_meta_info(
                                ods_connection_meta, 'etl_meta_info', sys_etl_meta_info_id,
                                datetime.datetime.now(datetime.timezone.utc),
                                f"Selecting source data to be updated finished: collected {stage_cur.rowcount}. "
                                f"Trying to create temp table in target for source data."
                            )

                            # Creating temp table like source. It's used to insert new values using PostgreSQL, not python
                            ods_cur.execute(create_table_ddl(stage_source_table_name, stage_source_table_ddl, is_temp=True))
                            while True:
                                rows = stage_cur.fetchmany(50000)
                                if not rows:
                                    break
                                execute_values(ods_cur, f"INSERT INTO {stage_source_table_name} VALUES %s", rows)

                            update_meta_info(
                                ods_connection_meta, 'etl_meta_info', sys_etl_meta_info_id,
                                datetime.datetime.now(datetime.timezone.utc),
                                "Temp table in target for source data was created succesfully. "
                                "Target dimension tables updating by new values..."
                            )

                            # Starting ODS dimension tables updating by new values.
                            for ods_table in stage_ods_dim_map:
                                # Selecting values from stage, which are not represented in ods dim tables
                                sqltemp = f"""
                                    SELECT stage.dim
                                    FROM (
                                        SELECT DISTINCT {stage_ods_dim_map[ods_table]['stage']} as dim 
                                        FROM {stage_source_table_name}
                                    ) stage
                                    LEFT JOIN (
                                        SELECT DISTINCT {stage_ods_dim_map[ods_table]['ods']} as dim 
                                        FROM {ods_table}
                                    ) ods 
                                    USING (dim)
                                    WHERE ods.dim IS NULL
                                """
                                # Inserting new values in ODS dim tables
                                ods_cur.execute(sqltemp)
                                execute_values(
                                    ods_cur,
                                    f"""
                                    INSERT INTO {ods_table} 
                                    ({stage_ods_dim_map[ods_table]['ods']}{', year, month, day' if ods_table == 'sales date' else ''}) 
                                    VALUES %s
                                    """,
                                    [(val, val.year, val.month, val.day) for val in ods_cur.fetchall()] if ods_table == 'sales date'
                                    else ods_cur.fetchall()
                                )

                            update_meta_info(
                                ods_connection_meta, 'etl_meta_info', sys_etl_meta_info_id,
                                datetime.datetime.now(datetime.timezone.utc),
                                "Target dimension tables were updated successfully. Updating values in target fact table..."
                            )

                            # Updating new values in ODS fact table.
                            joins = " \n".join(
                                [f'JOIN {item} ON {item}.{stage_ods_dim_map[item]["ods"]} = t.{stage_ods_dim_map[item]["stage"]}'
                                 for
                                 item in stage_ods_dim_map])
                            sqltemp = f"""
                                WITH source as (
                                    SELECT 
                                        {dim_ids_aliases},
                                        quantity_ordered,
                                        price_per_each,
                                        price_per_each * quantity_ordered as total_price,
                                        t.updated_at,
                                        {sys_etl_meta_info_id} as sys_etl_meta_info_id,
                                        t.id 
                                    FROM {stage_source_table_name} as t 
                                    {joins}
                                )
                                UPDATE sales_info 
                                SET (
                                    {fact_col_ids}, 
                                    quantity, 
                                    price_per_each, 
                                    total_price,
                                    source_updated_at,
                                    sys_etl_meta_info_id
                                ) = (
                                    SELECT 
                                        {fact_col_ids},
                                        quantity_ordered,
                                        price_per_each,
                                        total_price,
                                        updated_at,
                                        sys_etl_meta_info_id
                                    FROM source
                                    WHERE source.id = sales_info.source_id
                                )
                                WHERE sales_info.source_id IN (SELECT id FROM source)
                            """
                            ods_cur.execute(sqltemp)
                            count_updated = ods_cur.rowcount
                            update_meta_info(
                                ods_connection_meta, 'etl_meta_info', sys_etl_meta_info_id,
                                datetime.datetime.now(datetime.timezone.utc),
                                f"Updating values in target fact table finished: {count_updated} facts."
                            )
                        else:
                            update_meta_info(
                                ods_connection_meta, 'etl_meta_info', sys_etl_meta_info_id,
                                datetime.datetime.now(datetime.timezone.utc),
                                "No data to update."
                            )
            except (InsertEtlMetaError, UpdateEtlMetaError) as e:
                raise e
            except Exception as e:
                update_meta_info(
                    ods_connection_meta, 'etl_meta_info', sys_etl_meta_info_id,
                    datetime.datetime.now(datetime.timezone.utc),
                    f"Error while trying to update data:{e}",
                    is_error=True,
                )
                raise EtlError

            update_meta_info(
                ods_connection_meta, 'etl_meta_info', sys_etl_meta_info_id,
                datetime.datetime.now(datetime.timezone.utc),
                f"ETL finished.",
                is_finished=True
            )

    except StartEtlError as error:
        print(f"{error}\nETL won't be stared.")
    except (psycopg2.Error, InsertEtlMetaError, UpdateEtlMetaError, EtlError) as error:
        print(f"{error}\nETL terminated.")
