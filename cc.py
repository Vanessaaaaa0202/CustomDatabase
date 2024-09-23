from prompt_toolkit import prompt
from db1 import CustomDatabase
import json


class cc:
    def __init__(self):
        self.db = CustomDatabase()

    @staticmethod
    def is_float(value):
        try:
            float(value)
            return '.' in value
        except ValueError:
            return False

    @staticmethod
    def str_to_bool(str_value):
        if str_value == 'True':
            return True
        elif str_value == 'False':
            return False

    def parse_condition(self, condition_str):
        # condition_str: "column operator value"
        try:
            column, operator, value_str = condition_str.split()
            value = float(value_str) if self.is_float(value_str) else int(
                value_str) if value_str.isdigit() else value_str
            return column, operator, value
        except ValueError as e:
            print(f"Error parsing condition: {e}")
            return None, None, None

    def callkaidi(self, user_input, limit=None, condition_str=None, columns_str=None):

        if user_input == 'exit':
            return

        # Command: create_database database_name
        elif user_input.startswith('create_database'):
            _, dbname = user_input.split()
            return self.db.create_database(dbname)

        # Command: create_table table_name column1 type1, column2 type2...
        elif user_input.startswith('create_table'):
            _, tablename, schema = user_input.split(maxsplit=2)
            return self.db.create_table(tablename, schema)

        # Command: delete_table table_name
        elif user_input.startswith('delete_table'):
            _, tablename = user_input.split()
            return self.db.delete_table(tablename)

        # Command: load_database database_name
        elif user_input.startswith('load_database'):
            _, dbname = user_input.split()
            return self.db.load_existing_database(dbname)

        # Command: insert_csv table_name csv_file
        elif user_input.startswith('insert_csv'):
            _, tablename, file = user_input.split(maxsplit=2)
            return self.db.insert_data_from_csv(tablename, file)

        # Command: insert_row table_name {column1: value1, column2: value2,...}
        elif user_input.startswith('insert_row'):
            _, tablename, data = user_input.split(maxsplit=2)
            row_data = json.loads(data.replace('\'', '\"'))
            return self.db.insert_single_row(tablename, row_data)

        # Command: delete_row tablename key operator value
        # key(str) is specified column name
        # operator is condition such as ==, >, <..
        # value is the specified value
        elif user_input.startswith('delete_row'):
            _, tablename, key, operator, value = user_input.split(maxsplit=4)
            condition = self.db.create_condition(key, float(value) if self.is_float(value) else int(
                value) if value.isdigit() else value, operator)
            self.db.delete_data(tablename, condition)
            return self.db.delete_data(tablename, condition)

        # Command: update_row tablename key operator value to new_value
        # key(str) is specified column name
        # operator is condition such as ==, >, <..
        # value is the specified value you want to change
        elif user_input.startswith('update_row'):
            parts = user_input.split(' to ')
            part1 = parts[0]
            new_value = parts[1]
            _, tablename, key, operator, value = part1.split(maxsplit=4)
            condition = self.db.create_condition(key, float(value) if self.is_float(value) else int(
                value) if value.isdigit() else value, operator)
            return self.db.update_data(tablename, condition, {key: new_value})


        # Command: show_data table_name
        elif user_input.startswith('show_data'):
            limit = int(limit) if limit and limit.isdigit() else None
            condition = self.parse_condition(condition_str) if condition_str else None
            columns = columns_str.split(',') if columns_str else None
            parts = user_input.split()
            table_name = parts[1]
            data_to_show, success = self.db.show_data(table_name, limit, condition, columns)
            if success:
                return data_to_show
            else:
                return "Error or no data found."

        # Command: sum table_name new_table_name column_name
        # groupby_column is optional, used after groupby.
        elif user_input.startswith('sum'):
            parts = user_input.split()
            source_table_name = parts[1]
            new_table_name = parts[2]
            sum_column = parts[3]
            groupby_column = None if len(parts) < 5 else parts[4]
            return self.db.sum(source_table_name, new_table_name, sum_column, groupby_column)

        # Command: count table_name new_table_name
        # groupby_column is optional, used after groupby.
        elif user_input.startswith('count'):
            parts = user_input.split()
            source_table_name = parts[1]
            new_table_name = parts[2]
            groupby_column = None if len(parts) < 4 else parts[3]
            return self.db.count(source_table_name, new_table_name, groupby_column)

        # Command: avg table_name new_table_name avg_column
        # groupby_column is optional, used after groupby.
        elif user_input.startswith('avg'):
            parts = user_input.split()
            source_table_name = parts[1]
            new_table_name = parts[2]
            avg_column = parts[3]
            groupby_column = None if len(parts) < 5 else parts[4]
            return self.db.avg(source_table_name, new_table_name, avg_column, groupby_column)

        # Command: max table_name new_table_name max_column
        # groupby_column is optional, used after groupby.
        elif user_input.startswith('max'):
            parts = user_input.split()
            source_table_name = parts[1]
            new_table_name = parts[2]
            max_column = parts[3]
            groupby_column = None if len(parts) < 5 else parts[4]
            return self.db.max(source_table_name, new_table_name, max_column, groupby_column)

        # Command: min table_name new_table_name min_column
        # groupby_column is optional, used after groupby.
        elif user_input.startswith('min'):
            parts = user_input.split()
            source_table_name = parts[1]
            new_table_name = parts[2]
            min_column = parts[3]
            groupby_column = None if len(parts) < 5 else parts[4]
            return self.db.min(source_table_name, new_table_name, min_column, groupby_column)

        # Command: groupby table_name new_table_name groupby_column
        elif user_input.startswith('groupby'):
            parts = user_input.split()
            source_table_name = parts[1]
            new_table_name = parts[2]
            column = parts[3]
            self.db.groupby(source_table_name, new_table_name, column)
            # input: max/min/sum/avg column_name new_tablename or count new_tablename
            agg_response = prompt(
                'db> Using aggregation? (e.g. max/min/sum/avg column_name new_tablename or count new_tablename or no): ')
            if agg_response.lower() == 'no':
                show_response = prompt('db> Show data? (yes/no): ')
                if show_response.lower() == 'yes':
                    limit_response = prompt('db>show> Set limit? (limit e.g., 2/no): ')
                    limit = None
                    if limit_response.lower() != 'no':
                        limit = int(limit_response)

                    condition_response = prompt('db>show> Set condition? (condition e.g., Year > 2000/no): ')
                    condition = None
                    if condition_response.lower() != 'no':
                        key, operator, value = condition_response.split(maxsplit=2)
                        condition = self.db.create_condition(key, float(value) if self.is_float(value) else int(
                            value) if value.isdigit() else value, operator)

                    columns_response = prompt('db>show> Specify columns? (cloumns separated by comma/no): ')
                    columns = None
                    if columns_response.lower() != 'no':
                        columns = columns_response.split(',')
                    self.db.show_data(new_table_name, limit, condition, columns)
            if agg_response.startswith('max'):
                parts = agg_response.split()
                source_table_name = new_table_name
                new_table_name1 = parts[2]
                max_column = parts[1]
                groupby_column = column
                self.db.max(source_table_name, new_table_name1, max_column, groupby_column)
                show_response = prompt('db> Show data? (yes/no): ')
                if show_response.lower() == 'yes':
                    limit_response = prompt('db>show> Set limit? (limit e.g., 2/no): ')
                    limit = None
                    if limit_response.lower() != 'no':
                        limit = int(limit_response)

                    condition_response = prompt('db>show> Set condition? (condition e.g., Year > 2000/no): ')
                    condition = None
                    if condition_response.lower() != 'no':
                        key, operator, value = condition_response.split(maxsplit=2)
                        condition = self.db.create_condition(key, float(value) if self.is_float(value) else int(
                            value) if value.isdigit() else value, operator)

                    columns_response = prompt('db>show> Specify columns? (cloumns separated by comma/no): ')
                    columns = None
                    if columns_response.lower() != 'no':
                        columns = columns_response.split(',')
                    self.db.show_data(new_table_name1, limit, condition, columns)
            if agg_response.startswith('min'):
                parts = agg_response.split()
                source_table_name = new_table_name
                new_table_name1 = parts[2]
                min_column = parts[1]
                groupby_column = column
                self.db.min(source_table_name, new_table_name1, min_column, groupby_column)
                show_response = prompt('db> Show data? (yes/no): ')
                if show_response.lower() == 'yes':
                    limit_response = prompt('db>show> Set limit? (limit e.g., 2/no): ')
                    limit = None
                    if limit_response.lower() != 'no':
                        limit = int(limit_response)

                    condition_response = prompt('db>show> Set condition? (condition e.g., Year > 2000/no): ')
                    condition = None
                    if condition_response.lower() != 'no':
                        key, operator, value = condition_response.split(maxsplit=2)
                        condition = self.db.create_condition(key, float(value) if self.is_float(value) else int(
                            value) if value.isdigit() else value, operator)

                    columns_response = prompt('db>show> Specify columns? (cloumns separated by comma/no): ')
                    columns = None
                    if columns_response.lower() != 'no':
                        columns = columns_response.split(',')
                    self.db.show_data(new_table_name1, limit, condition, columns)
            if agg_response.startswith('sum'):
                parts = agg_response.split()
                source_table_name = new_table_name
                new_table_name1 = parts[2]
                sum_column = parts[1]
                groupby_column = column
                self.db.sum(source_table_name, new_table_name1, sum_column, groupby_column)
                show_response = prompt('db> Show data? (yes/no): ')
                if show_response.lower() == 'yes':
                    limit_response = prompt('db>show> Set limit? (limit e.g., 2/no): ')
                    limit = None
                    if limit_response.lower() != 'no':
                        limit = int(limit_response)

                    condition_response = prompt('db>show> Set condition? (condition e.g., Year > 2000/no): ')
                    condition = None
                    if condition_response.lower() != 'no':
                        key, operator, value = condition_response.split(maxsplit=2)
                        condition = self.db.create_condition(key, float(value) if self.is_float(value) else int(
                            value) if value.isdigit() else value, operator)

                    columns_response = prompt('db>show> Specify columns? (cloumns separated by comma/no): ')
                    columns = None
                    if columns_response.lower() != 'no':
                        columns = columns_response.split(',')
                    self.db.show_data(new_table_name1, limit, condition, columns)
            if agg_response.startswith('avg'):
                parts = agg_response.split()
                source_table_name = new_table_name
                new_table_name1 = parts[2]
                avg_column = parts[1]
                groupby_column = column
                self.db.avg(source_table_name, new_table_name1, avg_column, groupby_column)
                show_response = prompt('db> Show data? (yes/no): ')
                if show_response.lower() == 'yes':
                    limit_response = prompt('db>show> Set limit? (limit e.g., 2/no): ')
                    limit = None
                    if limit_response.lower() != 'no':
                        limit = int(limit_response)

                    condition_response = prompt('db>show> Set condition? (condition e.g., Year > 2000/no): ')
                    condition = None
                    if condition_response.lower() != 'no':
                        key, operator, value = condition_response.split(maxsplit=2)
                        condition = self.db.create_condition(key, float(value) if self.is_float(value) else int(
                            value) if value.isdigit() else value, operator)

                    columns_response = prompt('db>show> Specify columns? (cloumns separated by comma/no): ')
                    columns = None
                    if columns_response.lower() != 'no':
                        columns = columns_response.split(',')
                    self.db.show_data(new_table_name1, limit, condition, columns)
            if agg_response.startswith('count'):
                parts = agg_response.split()
                source_table_name = new_table_name
                new_table_name1 = parts[1]
                groupby_column = column
                self.db.count(source_table_name, new_table_name1, groupby_column)
                show_response = prompt('db> Show data? (yes/no): ')
                if show_response.lower() == 'yes':
                    limit_response = prompt('db>show> Set limit? (limit e.g., 2/no): ')
                    limit = None
                    if limit_response.lower() != 'no':
                        limit = int(limit_response)

                    condition_response = prompt('db>show> Set condition? (condition e.g., Year > 2000/no): ')
                    condition = None
                    if condition_response.lower() != 'no':
                        key, operator, value = condition_response.split(maxsplit=2)
                        condition = self.db.create_condition(key, float(value) if self.is_float(value) else int(
                            value) if value.isdigit() else value, operator)

                    columns_response = prompt('db>show> Specify columns? (cloumns separated by comma/no): ')
                    columns = None
                    if columns_response.lower() != 'no':
                        columns = columns_response.split(',')
                    self.db.show_data(new_table_name1, limit, condition, columns)

        # Command: ordering_data table_name new_table_name column True/False
        elif user_input.startswith('ordering_data'):
            _, source_table_name, new_table_name, column, order = user_input.split(maxsplit=4)
            new_order = self.str_to_bool(order)
            return self.db.order_data(source_table_name, new_table_name, column, ascending=new_order)

        # Command: join table1_name table2_name new_table_name table1_join_column table2_join_join_column
        elif user_input.startswith('join'):
            _, table1_name, table2_name, new_table_name, table1_join_key, table2_join_key = user_input.split(maxsplit=5)
            return self.db.join(table1_name, table2_name, new_table_name, table1_join_key, table2_join_key)

        else:
            print("Unknown command.")
