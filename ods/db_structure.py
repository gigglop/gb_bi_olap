from dotenv import dotenv_values
import psycopg2
from ddl_func import create_table_ddl, add_fk_ddl


sales_tables_ddl = {

    "sales_order": {
        "id": "BIGSERIAL PRIMARY KEY",
        "order_id": "INT UNIQUE"
    },

    "sales_product": {
        "id": "BIGSERIAL PRIMARY KEY",
        "name": "VARCHAR UNIQUE"
    },

    "sales_date": {
        "id": "BIGSERIAL PRIMARY KEY",
        "ts": "TIMESTAMP UNIQUE",
        "year": "INT",
        "month": "INT",
        "day": "INT"
    },

    "sales_purchase_address": {
        "id": "BIGSERIAL PRIMARY KEY",
        "address": "TEXT UNIQUE"
    },

    "sales_info": {
            "order_id": "BIGINT",
            "product_id": "BIGINT",
            "purchase_address_id": "BIGINT",
            "datetime_id": "BIGINT",
            "quantity": "INT CONSTRAINT positive_quantity CHECK (quantity > 0)",
            "price_per_each": "NUMERIC CONSTRAINT positive_price_per_each CHECK (price_per_each > 0)",
            "total_price": "NUMERIC CONSTRAINT positive_total_price CHECK (total_price > 0)",
            "sys_id": "BIGSERIAL PRIMARY KEY NOT NULL",
            "sys_created_at": "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
            "sys_updated_at": "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
            "sys_etl_meta_info_id": "BIGINT",
            "source_id": "BIGINT NOT NULL",
            "source_created_at": "TIMESTAMP NOT NULL",
            "source_updated_at": "TIMESTAMP NOT NULL"
    }
}

meta_info_tables_ddl = {
    "etl_meta_info": {
            "id": "BIGSERIAL PRIMARY KEY",
            "start_date": "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
            "end_date": "TIMESTAMP",
            "state": "VARCHAR NOT NULL",
            "source": "TEXT NOT NULL",
            "target": "TEXT NOT NULL",
            "log": "TEXT"
    }
}


references = {
    "sales_info": [
            {
                "referencing_columns": ("order_id",),
                "referenced_table": "sales_order",
                "referenced_columns": ("id",)
            },
            {
                "referencing_columns": ("product_id",),
                "referenced_table": "sales_product",
                "referenced_columns": ("id",)
            },
            {
                "referencing_columns": ("datetime_id",),
                "referenced_table": "sales_date",
                "referenced_columns": ("id",)
            },
            {
                "referencing_columns": ("purchase_address_id",),
                "referenced_table": "sales_purchase_address",
                "referenced_columns": ("id",)
            },
            {
                "referencing_columns": ("sys_etl_meta_info_id",),
                "referenced_table": "etl_meta_info",
                "referenced_columns": ("id",)
            }
        ]
    }

if __name__ == "__main__":
    try:
        # Connect to an ods database
        connection = psycopg2.connect(
            host=dotenv_values().get("ODS_POSTGRES_HOST"),
            port=dotenv_values().get("ODS_POSTGRES_PORT"),
            database=dotenv_values().get("ODS_POSTGRES_DB"),
            user=dotenv_values().get("ODS_POSTGRES_USER"),
            password=dotenv_values().get("ODS_POSTGRES_PASS")
        )
        with connection:
            with connection.cursor() as cur:
                tables_to_create = dict(sales_tables_ddl, **meta_info_tables_ddl)
                for table in tables_to_create:
                    cur.execute(create_table_ddl(table, tables_to_create[table]))
                    print(f"Table '{table}' is ready to use in PostgreSQL.")
                for referencing_table in references:
                    cur.execute(add_fk_ddl(referencing_table, references[referencing_table]))
                    referenced_tables = ', '.join([ref['referenced_table'] for ref in references[referencing_table]])
                    print(f"Table '{referencing_table}' was referenced to tables: {referenced_tables}")
                cur.execute("""
                    CREATE OR REPLACE VIEW Orders AS
                    SELECT 
                        so.order_id AS "Order",
                        sp.name AS "Product",
                        spa.address AS "Purchase Address",
                        sd.ts AS "Order Full Date",
                        sd.year AS "Order Year",
                        sd.month AS "Order Month",
                        sd.day AS "Order Day",
                        si.quantity AS "Product Quantity",
                        si.price_per_each AS "Product Price",
                        si.total_price AS "Order Full Price"
                    FROM sales_info si
                    JOIN sales_product sp ON si.product_id = sp.id
                    JOIN sales_order so ON si.order_id = so.id
                    JOIN sales_purchase_address spa ON si.purchase_address_id = spa.id
                    JOIN sales_date sd ON si.datetime_id = sd.id
                """)
                print("View 'Orders' successfully defined.")
    except (Exception, psycopg2.Error) as error:
        print("Error while creating tables: ", error)
