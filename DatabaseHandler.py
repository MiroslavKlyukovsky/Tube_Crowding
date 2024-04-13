import psycopg2


class DatabaseHandler:
    def __init__(self, current_crowding_data_table, max_rows, db_params):
        self.current_crowding_data_table = current_crowding_data_table
        self.max_rows = max_rows
        self.rows_left = 0
        self.stations_sequence = ()
        self.db_params = db_params
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.disconnect()
            self.connection = psycopg2.connect(**self.db_params)
            self.cursor = self.connection.cursor()
        except Exception as err:
            raise Exception(f"[connect] {err}")

    def disconnect(self):
        try:
            if self.cursor is not None:
                self.cursor.close()
            if self.connection is not None:
                self.connection.close()
            self.cursor = None
            self.connection = None
        except Exception as err:
            raise Exception(f"[disconnect] {err}")

    def set_up_table(self, naptan_ids, timestamp):
        try:
            if self.current_crowding_data_table is None or self.rows_left == 0 or self.stations_sequence != naptan_ids:
                new_table_name = f"crowding_data_{timestamp.replace('-', '_').replace(':', '_').replace(' ', '_')}"
                column_definitions = ', '.join([f'"{n_id}" NUMERIC(5,4)' for n_id in naptan_ids])

                create_table_query = f"""
                                        CREATE TABLE {new_table_name} (
                                            c_timestamp TIMESTAMP PRIMARY KEY,
                                            {column_definitions}
                                        );
                                      """

                self.cursor.execute(create_table_query)

                self.stations_sequence = naptan_ids
                self.rows_left = self.max_rows
                self.current_crowding_data_table = new_table_name
                return True
            else:
                return False
        except Exception as err:
            raise Exception(f"[set_up_table] {err}")

    def insert_row_in_table(self, timestamp, data):
        try:
            filtered_naptanids = [n_id for n_id, value in data.items() if str(value) != None]

            insert_query = f"""
            INSERT INTO {self.current_crowding_data_table} (c_timestamp, {', '.join([f'"{n_id}"' for n_id in filtered_naptanids])})
            VALUES (%s, {', '.join(['%s' for _ in filtered_naptanids])})
            """

            self.cursor.execute(insert_query, [timestamp] + [data[n_id] for n_id in filtered_naptanids])

            return True
        except Exception as err:
            raise Exception(f"[insert_row_in_table] {err}")

    def insert_rows_in_table(self, dumper):
        try:
            self.connect()
            for timestamp, data in dumper:
                naptan_ids = tuple(sorted(list(data.keys())))
                if self.rows_left == 0 or self.stations_sequence != naptan_ids:
                    self.connection.commit()
                    if self.set_up_table(naptan_ids, timestamp):
                        self.connection.commit()
                self.insert_row_in_table(timestamp, data)
                self.rows_left -= 1
            self.connection.commit()
            self.disconnect()
        except Exception as err:
            raise Exception(f"[insert_rows_in_table] {err}")
