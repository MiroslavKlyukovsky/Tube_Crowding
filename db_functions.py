def give_verdict(cursor, naptan_ids, current_crowding_data_table, max_rows):
    try:
        print(1)
        create_new_table = False
        if current_crowding_data_table:
            print(2)
            cursor.execute(f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = '{current_crowding_data_table}')")
            table_exists = cursor.fetchone()[0]
            if table_exists:
                print(3)
                query = f"SELECT COUNT(*) FROM {current_crowding_data_table};"
                cursor.execute(query)
                row_count = cursor.fetchone()[0]

                diff_naptanIds = False
                cursor.execute(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name = '{current_crowding_data_table}' AND column_name != 'c_timestamp' ORDER BY column_name;")
                existing_columns = [column[0] for column in cursor.fetchall()]
                if existing_columns != naptan_ids:
                    print(existing_columns, '\n', naptan_ids)
                    print(4)
                    diff_naptanIds = True

                if row_count >= max_rows or diff_naptanIds:
                    print(5)
                    create_new_table = True
                else:
                    create_new_table = False
            else:
                create_new_table = True
        else:
            create_new_table = True

        return create_new_table
    except Exception as error:
        raise Exception(f"When deciding table creation: {error}")


def set_up_table(cursor, naptan_ids, timestamp, current_crowding_data_table, max_rows):
    try:
        create_new_table = give_verdict(cursor, naptan_ids, current_crowding_data_table, max_rows)

        if create_new_table:
            print(6)
            new_table_name = f"crowding_data_{timestamp.replace('-', '_').replace(':', '_').replace(' ', '_')}"

            column_definitions = ', '.join([f'"{n_id}" NUMERIC(5,4)' for n_id in naptan_ids])

            create_table_query = f"""
                                    CREATE TABLE {new_table_name} (
                                        c_timestamp TIMESTAMP PRIMARY KEY,
                                        {column_definitions}
                                    );
                                  """
            cursor.execute(create_table_query)
            return new_table_name
        else:
            return current_crowding_data_table
    except Exception as error:
        raise Exception(f"While creating table: {error}")


def insert_row_in_table(cursor, timestamp, data, current_crowding_data_table):
    try:
        filtered_naptanids = [n_id for n_id, value in data.items() if str(value) != None]

        insert_query = f"""
                                INSERT INTO {current_crowding_data_table} (c_timestamp, {', '.join([f'"{n_id}"' for n_id in filtered_naptanids])})
                                VALUES (%s, {', '.join(['%s' for _ in filtered_naptanids])})
                            """

        cursor.execute(insert_query, [timestamp] + [data[n_id] for n_id in filtered_naptanids])

        return True
    except Exception as error:
        raise Exception(f"While inserting row in table: {error}")
