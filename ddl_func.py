# TODO: Добавить классы
#  'колонка'
#  'ограничение'
#  'таблица'
#  'БД'
#  'ETL'
# Почитать про ORM



def create_table_ddl(table_name: str, table_dict: dict, is_temp=False) -> str:
    """
    Function generates SQL statement for table creating.
    :param table_name: name of table to be created
    :param table_dict: dictionary with columns and their definitions. Dictionary format can get from example.
    DO NOT define references. Better to create tables and then create refs. See also 'add_reference' function.
    :param is_temp: is table temporary?
    :return: sql (ddl)

    Example
        in:
        - table_name='t'
        - table_dict={
                "id": "INT PRIMARY KEY",
                "desc": "VARCHAR NOT NULL"
            }
        - is_temp=True

        out:
            "CREATE TEMP TABLE IF NOT EXISTS t (
              id INT PRIMARY KEY,
              desc VARCHAR NOT NULL
            )"
    },

    """
    sql_line_join_sep = ", \n"
    sql =  \
        f"CREATE{' TEMP ' if is_temp else ' '}TABLE IF NOT EXISTS {table_name} (\n" \
        f"{sql_line_join_sep.join([f'  {column} {column_ddl}' for column, column_ddl in table_dict.items()])}" \
        f"\n)"
    return sql


def add_fk_ddl(table_name: str, ref_list: list) -> str:
    """
    Function generates SQL statement for adding foreign constraints
    :param table_name: name of referencing table
    :param ref_list: list with references definitions, see format in example below
    :return: sql (ddl)

    Example
        in:
            -table_name='t'
            -ref_list=[
                {
                    "referencing_columns": ("col1",),
                    "referenced_table": "ref_t1",
                    "referenced_columns": ("ref_col1",)
                },
                {
                    "referencing_columns": ("col2", "col3"),
                    "referenced_table": "ref_t2",
                    "referenced_columns": ("ref_col2", "ref_col2")
                }
            ]

        out:
            "ALTER TABLE t
                ADD FOREIGN KEY (col1) REFERENCES ref_t1 (ref_col1),
                ADD FOREIGN KEY (col2,col3) REFERENCES ref_t2 (ref_col2, ref_col2)"


    """
    sql_line_join_sep = ", \n"
    fk = sql_line_join_sep.join(
            [
            f"\tADD FOREIGN KEY ({', '.join(reference.get('referencing_columns'))}) " \
            f"REFERENCES {reference.get('referenced_table')} " \
            f"({', '.join(reference.get('referenced_columns'))})"
            for reference in ref_list])
    return f"ALTER TABLE {table_name} \n{fk}"


def add_unique_constr_ddl(table_name: str, col_lists: tuple) -> str:
    """
        Function generates SQL statement for adding unique constraints
        :param table_name: name of table
        :param col_list: tuple of lists, nested lists have to contains columns to be unique in conjuction
        :return: sql (ddl)

        Example
            in:
                -table_name='t'
                -ref_list=(["col1"], ["col2", "col3"])

            out:
                "ALTER TABLE t
                    ADD CONSTRAINT UNIQUE (col1),
                    ADD CONSTRAINT UNIQUE (col2, col3)"
        """
    sql_line_join_sep = ", \n"
    constr = sql_line_join_sep.join(
        [
            f"\tADD UNIQUE ({', '.join(col_list)})" \
            for col_list in col_lists
        ]
    )
    return f"ALTER TABLE {table_name} \n{constr}"


if __name__ == '__main__':
    pass
