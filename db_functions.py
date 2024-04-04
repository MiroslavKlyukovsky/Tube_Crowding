import psycopg2
from config import db_params


def create_crowding_table(cursor, naptan_ids, timestamp, CURRENT_CROWDING_DATA_TABLE):
    try:
        print(1)
        create_new_table = False
        if CURRENT_CROWDING_DATA_TABLE is not None:
            print(2)
            cursor.execute(f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = '{CURRENT_CROWDING_DATA_TABLE}')")
            table_exists = cursor.fetchone()[0]
            if table_exists:
                print(3)
                query = f"SELECT COUNT(*) FROM {CURRENT_CROWDING_DATA_TABLE};"
                cursor.execute(query)
                row_count = cursor.fetchone()[0]

                diff_naptanIds = False
                cursor.execute(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name = '{CURRENT_CROWDING_DATA_TABLE}' AND column_name != 'c_timestamp' ORDER BY column_name;")
                existing_columns = [column[0] for column in cursor.fetchall()]
                if existing_columns != naptan_ids:
                    print(existing_columns, '\n', naptan_ids)
                    print(4)
                    diff_naptanIds = True

                if row_count >= 500 or diff_naptanIds:
                    print(5)
                    create_new_table = True
                else:
                    create_new_table = False
            else:
                create_new_table = True
        else:
            create_new_table = True

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
            return CURRENT_CROWDING_DATA_TABLE
    except Exception as error:
        raise Exception(f"Error creating table: {error}")


def save_to_postgresql(dumper_state, email_informant, CURRENT_CROWDING_DATA_TABLE):
    try:
        if CURRENT_CROWDING_DATA_TABLE:
            temp_current = CURRENT_CROWDING_DATA_TABLE
        else:
            temp_current = None

        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        for timestamp, data in dumper_state.items():
            naptan_ids = sorted(list(data.keys()))

            table_name = create_crowding_table(cursor, naptan_ids, timestamp, temp_current)
            if table_name != temp_current:
                connection.commit()
                temp_current = table_name
                email_informant.send_email("New table", f"The new table names is: {temp_current}")
                print("New table", f"The new table names is: {temp_current}")

            filtered_naptanids = [n_id for n_id, value in data.items() if str(value) != None]

            insert_query = f"""
                            INSERT INTO {temp_current} (c_timestamp, {', '.join([f'"{n_id}"' for n_id in filtered_naptanids])})
                            VALUES (%s, {', '.join(['%s' for _ in filtered_naptanids])})
                        """

            cursor.execute(insert_query, [timestamp] + [data[n_id] for n_id in filtered_naptanids])
            connection.commit()

        return temp_current
    except Exception as error:
        raise Exception(f"Error in save_to_postgresql: {error}")
    finally:
        cursor.close()
        connection.close()
