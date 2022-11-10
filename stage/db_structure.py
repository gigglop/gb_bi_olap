from dotenv import dotenv_values
import psycopg2
from ddl_func import add_fk_ddl, add_unique_constr_ddl, create_table_ddl

source_table_name = dotenv_values().get("STAGE_SALES_SOURCE_TABLE")
meta_data_table_name = dotenv_values().get("STAGE_POSTGRES_DB_META_TABLE")

table_ddl = {
    f"{source_table_name}": {
            "id": "BIGSERIAL PRIMARY KEY",
            "order_id": "INT",
            "product": "VARCHAR",
            "quantity_ordered": "INT",
            "price_per_each": "DECIMAL",
            "order_date": "TIMESTAMP",
            "purchase_address": "TEXT",
            "created_at": "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
            "updated_at": "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
            "etl_meta_info_id": "BIGINT"
    },
    f"{meta_data_table_name}": {
            "id": "BIGSERIAL PRIMARY KEY",
            "start_date": "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
            "end_date": "TIMESTAMP",
            "state": "VARCHAR NOT NULL",
            "source": "TEXT NOT NULL",
            "target_table": "VARCHAR NOT NULL",
            "log": "TEXT"
    }
}

references = {
    f"{source_table_name}": [
            {
                "referencing_columns": ("etl_meta_info_id",),
                "referenced_table": f"{meta_data_table_name}",
                "referenced_columns": ("id",)
            }
        ]
}

unique_constr = {
    # f"{source_table_name}": (
    #     [
    #         "order_id", "product", "quantity_ordered",
    #         "price_per_each", "order_date", "purchase_address"
    #     ],
    # )
}
if __name__ == '__main__':
    try:
        connection = psycopg2.connect(
            host=dotenv_values().get("STAGE_POSTGRES_HOST"),
            port=dotenv_values().get("STAGE_POSTGRES_PORT"),
            database=dotenv_values().get("STAGE_POSTGRES_DB"),
            user=dotenv_values().get("STAGE_POSTGRES_USER"),
            password=dotenv_values().get("STAGE_POSTGRES_PASS")
        )

        with connection:
            with connection.cursor() as cur:
                for table in table_ddl:
                    cur.execute(create_table_ddl(table, table_ddl[table]))
                    print(f"Table '{table}' is ready to use in PostgreSQL.")
                for referencing_table in references:
                    cur.execute(add_fk_ddl(referencing_table, references[referencing_table]))
                    referenced_tables = ', '.join([ref['referenced_table'] for ref in references[referencing_table]])
                    print(f"Table '{referencing_table}' was referenced to tables: {referenced_tables}")
                for table in unique_constr:
                    cur.execute(add_unique_constr_ddl(table, unique_constr[table]))
                    uniq_cols = ', \n'.join(
                        [f"\t({', '.join(col_list)})" for col_list in unique_constr[table]]
                    )
                    print(f"Table '{table}' now has next unique consts:\n {uniq_cols}")

    except (Exception, psycopg2.Error) as error:
        print("Error while creating tables: ", error)
