import psycopg2
from utils import timestamp_format


class DatabaseHandler:
    """
        A class to handle database operations for crowding data.

        Attributes:
        - max_rows_in_commit (int): Maximum number of rows to insert in a single commit.
        - current_crowding_data_table (str): Name of the current crowding data table.
        - max_rows (int): Maximum number of rows allowed in a table.
        - db_params (dict): Database connection parameters.
        - connection: psycopg2 connection object.
        - cursor: psycopg2 cursor object.
        - rows_left (int): Number of rows left in the current table.
        - stations_sequence (tuple): Sequence of station IDs.
        - planned_to_insert (list): List of rows planned for insertion.
    """
    def __init__(self, max_rows_in_commit, current_crowding_data_table, max_rows, db_params):
        """
            Initializes the DatabaseHandler instance.

            Args:
            - max_rows_in_commit (int): Maximum number of rows to insert in a single commit.
            - current_crowding_data_table (str): Name of the current crowding data table.
            - max_rows (int): Maximum number of rows allowed in a table.
            - db_params (dict): Database connection parameters.
        """
        self.max_rows = max_rows
        self.current_crowding_data_table = current_crowding_data_table
        self.rows_left = 0
        self.stations_sequence = ()

        self.db_params = db_params
        self.connection = None
        self.cursor = None

        self.max_rows_in_commit = max_rows_in_commit
        self.planned_to_insert = []

        if self.current_crowding_data_table:
            self.connect()

            row_num_que = f"SELECT COUNT(*) FROM {self.current_crowding_data_table}"
            self.cursor.execute(row_num_que)
            row_count = self.cursor.fetchone()[0]
            self.rows_left = self.max_rows - row_count

            stations_seq_que = (f"SELECT column_name FROM information_schema.columns " +
                                f"WHERE table_name = '{current_crowding_data_table}' AND column_name != 'c_timestamp'" +
                                f" ORDER BY column_name;")
            self.cursor.execute(stations_seq_que)
            existing_columns = [column[0] for column in self.cursor.fetchall()]
            existing_columns = tuple(sorted(existing_columns))
            self.stations_sequence = existing_columns

            self.disconnect()

    def connect(self):
        """Establishes a connection to the database."""
        try:
            self.disconnect()
            self.connection = psycopg2.connect(**self.db_params)
            self.cursor = self.connection.cursor()
        except Exception as err:
            raise Exception(f"[connect] {err}")

    def disconnect(self):
        """Closes the database connection."""
        try:
            if self.cursor is not None:
                self.cursor.close()
            if self.connection is not None:
                self.connection.close()
            self.cursor = None
            self.connection = None
        except Exception as err:
            raise Exception(f"[disconnect] {err}")

    def create_table(self, naptan_ids, timestamp):
        """
            Creates a new table for crowding data.

            Args:
            - naptan_ids (list): List of station IDs.
            - timestamp (str): Timestamp for table naming.
        """
        try:
            new_table_name = f"crowding_data_{timestamp.replace('-', '_').replace(':', '_').replace(' ', '_')}"
            column_definitions = ', '.join([f'"{n_id}" NUMERIC(5,4)' for n_id in naptan_ids])

            create_table_query = f"""
                                    CREATE TABLE {new_table_name} (
                                        c_timestamp TIMESTAMP PRIMARY KEY,
                                        {column_definitions}
                                    );
                                  """

            self.cursor.execute(create_table_query)
            self.connection.commit()

            self.stations_sequence = naptan_ids
            self.rows_left = self.max_rows
            self.current_crowding_data_table = new_table_name
            return True
        except Exception as err:
            raise Exception(f"[create_table] {err}")

    def insert_planned_rows(self):
        """Inserts planned rows into the current crowding data table."""
        try:
            if len(self.planned_to_insert) == 0:
                return False

            values_list = []
            stations_to_insert = ', '.join([f'"{n_id}"' for n_id in self.stations_sequence])
            begin_insert_expr = f"""
                    INSERT INTO {self.current_crowding_data_table} (c_timestamp, {stations_to_insert})
                    VALUES
            """
            for timestamp, data in self.planned_to_insert:
                insert_row_values = ['%s' if data[n_id] is not None else 'NULL' for n_id in self.stations_sequence]
                insert_row_expr = f"""\t(TO_TIMESTAMP('{timestamp}','{timestamp_format}'), {', '.join(insert_row_values)})"""
                begin_insert_expr += insert_row_expr + ',\n'
                values_list.extend([data[n_id] for n_id in self.stations_sequence if data[n_id] is not None])
            insert_query = begin_insert_expr[:-2]

            self.cursor.execute(insert_query, values_list)
            self.connection.commit()

            self.rows_left -= len(self.planned_to_insert)
            self.planned_to_insert = []
            return True
        except Exception as err:
            raise Exception(f"[insert_multiple_rows] {err}")

    def insert_dumper(self, dumper):
        """
            Inserts data from the dumper into the database.

            Args:
            - dumper (list): List of data tuples (timestamp, data).
        """
        try:
            if len(dumper) == 0:
                raise Exception("Empty dumper")

            self.connect()

            while True:
                if len(dumper) == 0:
                    self.insert_planned_rows()
                    break

                timestamp, data = dumper.pop(0)
                naptan_ids = tuple(sorted(list(data.keys())))
                if not self.current_crowding_data_table or self.rows_left == 0 or self.stations_sequence != naptan_ids:
                    self.insert_planned_rows()
                    self.create_table(naptan_ids, timestamp)

                self.planned_to_insert.append((timestamp, data))

                the_length = len(self.planned_to_insert)
                if the_length >= self.max_rows_in_commit or self.rows_left == the_length:
                    self.insert_planned_rows()

            self.insert_planned_rows()
            self.disconnect()
        except Exception as err:
            raise Exception(f"[insert dumper] {err}")
        finally:
            self.disconnect()
