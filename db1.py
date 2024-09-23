import json
import os
import csv

class CustomDatabase:
    # Set chunk size to be 2000.
    def __init__(self, chunk_size=1000):
        self.database_name = None
        self.database_path = None
        self.chunk_size = chunk_size
        self.tables = {}

    # Create database as file folder
    def create_database(self, database_name):
        self.database_name = database_name
        self.database_path = f"./{database_name}"
        if not os.path.exists(self.database_path):
            os.makedirs(self.database_path)
            #print(f"Database '{database_name}' created.")
            return f"Database '{database_name}' created."
        else:
            #print(f"Database '{database_name}' already existed.")
            return f"Database '{database_name}' already existed."

    # Create table as file and save file in the database folder
    def create_table(self, table_name, schema): 
        if table_name not in self.tables:
            columns = {col.split()[0]: col.split()[1] for col in schema.split(',')}
            self.tables[table_name] = {'columns': columns, 'data': []}
            save_result = self.save_data(table_name)
            #print(f"Table '{table_name}' created with schema '{schema}' . {save_result}")
            return f"Table '{table_name}' created with schema '{schema}'. {save_result}"
        else:
            #print(f"Table '{table_name}' already existed in database '{self.database_name}'.")
            return f"Table '{table_name}' already existed in database '{self.database_name}'."

    # Delete table by table name
    def delete_table(self, table_name):
        table_file_path = os.path.join(self.database_path, f"{table_name}.json")
        if table_name in self.tables:
            try:
                del self.tables[table_name]
                os.remove(table_file_path)
                print(f"Table '{table_name}' has been deleted.")
                return f"Table '{table_name}' has been deleted."
            except Exception as e:
                print(f"Error deleting table '{table_name}': {e}")
                return f"Error deleting table '{table_name}': {e}"
        else:
            print(f"Table '{table_name}' does not exist.")
            return f"Table '{table_name}' does not exist."

    # def delete_table(self, table_name):
    #     if table_name in self.tables:
    #         del self.tables[table_name]
    #         os.remove(os.path.join(self.database_path, f"{table_name}.json"))
    #         print(f"Table '{table_name}' has been deleted.")
    #         return f"Table '{table_name}' has been deleted."
    #     else:
    #         print(f"Table '{table_name}' does not exist.")
    #         return f"Table '{table_name}' does not exist."
    def load_existing_database(self, database_name):
        self.database_name = database_name
        self.database_path = f"./{database_name}"
        self.tables = {}

        if not os.path.exists(self.database_path):
            #print(f"Database '{database_name}' does not exist。")
            return f"Database '{database_name}' does not exist。"

        #print(f"Loading '{database_name}'...")

        for filename in os.listdir(self.database_path):
            if filename.endswith('.json'):
                table_name = filename[:-5]
                self.load_data(table_name)

        return f"Loading '{database_name}'..."

    # Find the file with the name of table and load data into database system
    def load_data(self, table_name):
        file_path = os.path.join(self.database_path, f"{table_name}.json")
        self.tables[table_name] = {'data': [], 'columns': {}}  
        with open(file_path, 'r') as file:
            first_line = next(file).strip()
            if first_line:
                columns_data = json.loads(first_line)
                self.tables[table_name]['columns'] = columns_data.get('columns',{})

            for line in file:
                if line.strip(): 
                    chunk = json.loads(line)
                    self.tables[table_name]['data'].append(chunk)
        #print(f"Gained table'{table_name}' successfully。")
        return f"Gained table'{table_name}' successfully。"

    # Save the data into file
    def save_data(self, table_name):
        file_path = os.path.join(self.database_path, f"{table_name}.json")
        if table_name in self.tables:
            with open(file_path, 'w') as file:
                if 'columns' in self.tables[table_name]:
                    json.dump({'columns': self.tables[table_name]['columns']}, file)
                    file.write("\n")
                for chunk in self.tables[table_name]['data']:
                    json.dump(chunk, file)
                    file.write("\n")
            return f"Data for table '{table_name}' saved successfully."
        else:
            return f"Error: Table '{table_name}' does not exist and thus data could not be saved."

            
    def insert_data_from_csv(self, table_name, file_path):
        if table_name not in self.tables:
            #print(f"Table '{table_name}' does not exist.")
            return f"Table '{table_name}' does not exist."

        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            reader = csv.DictReader(file)
            chunk = []
            for row_data in reader:
                new_row_data = {}
                for column, value in row_data.items():
                    column_type = self.tables[table_name]['columns'].get(column)
                    if column_type == 'int':
                        try:
                            new_row_data[column] = int(value) if value else None
                        except ValueError:
                            new_row_data[column] = None
                    elif column_type == 'float':
                        try:
                            new_row_data[column] = float(value) if value else None
                        except ValueError:
                            new_row_data[column] = None
                    else:
                        new_row_data[column] = value
                chunk.append(new_row_data)

                if len(chunk) >= self.chunk_size:
                    self.tables[table_name]['data'].append(chunk)
                    chunk = []
            if chunk:
                self.tables[table_name]['data'].append(chunk)
            save_result = self.save_data(table_name)
            #print(f"Table '{table_name}' imported from csv successfully. {save_result}")
            return f"Table '{table_name}' imported from csv successfully. {save_result}"


    def insert_single_row(self, table_name, row_data):
        # Check if table exist
        if table_name not in self.tables:
            print(f"Table '{table_name}' does not exist.")
            return f"Table '{table_name}' does not exist."
        
        new_row_data = {}
        for column, column_type in self.tables[table_name]['columns'].items():
            value = row_data.get(column)
            # Check the data type of value
            if column_type == 'int':
                try:
                    new_row_data[column] = int(value) if value else None
                except ValueError:
                    new_row_data[column] = None
            elif column_type == 'float':
                try:
                    new_row_data[column] = float(value) if value else None
                except ValueError:
                    new_row_data[column] = None
            else:
                new_row_data[column] = value
        # Save data into chunks and save these chunk into one file
        if not self.tables[table_name]['data'] or len(self.tables[table_name]['data'][-1]) >= self.chunk_size:
            self.tables[table_name]['data'].append([new_row_data])
        else:
            self.tables[table_name]['data'][-1].append(new_row_data)
        save_result = self.save_data(table_name)
        #print(f"new single row inserted successfully. {save_result}")
        return f"new single row inserted successfully. {save_result}"

    # Define the condition for other function
    def create_condition(self, key, value, operator):
        def conditions(item):
            if operator == '==':
                return item.get(key, None) == value
            elif operator == '<':
                return item.get(key, None) < value
            elif operator == '<=':
                return item.get(key, None) <= value
            elif operator == '>':
                return item.get(key, None) > value
            elif operator == '>=':
                return item.get(key, None) >= value
            elif operator == '!=':
                return item.get(key, None) != value
            return False
        return conditions

    # Using the condition to select the data which we want to delete
    def delete_data(self, table_name, condition):
        # Check if table exist
        if table_name not in self.tables:
            #print(f"Table '{table_name}' does not exist.")
            return f"Table '{table_name}' does not exist."
        new_data = []
        for chunk in self.tables[table_name]['data']:
            new_chunk = [item for item in chunk if not condition(item)]
            new_data.append(new_chunk)
        self.tables[table_name]['data'] = new_data
        save_result = self.save_data(table_name)
        #print(f"data deleted successfully. {save_result}")
        return f"data deleted successfully. {save_result}"


    # Using the condition to select the data which we want to update
    def update_data(self, table_name, condition, update_values):
        # Check the data type of value
        def is_float(value):
            try:
                float(value)
                return '.' in value
            except ValueError:
                return False 
        if table_name not in self.tables:
            #print(f"Table '{table_name}' does not exist.")
            return f"Table '{table_name}' does not exist."
        #updated = False
        for chunk in self.tables[table_name]['data']:
            for item in chunk:
                if condition(item):
                    for key, value in update_values.items():
                        if key in item:
                            if is_float(value):
                                new_value = float(value)
                            elif value.isdigit():
                                new_value = int(value)
                            else:
                                new_value = value
                            item[key] = new_value
        #print(f"data updated successfully.")
        return f"data updated successfully."

    # Sort the table data and create the new table with new table name to store the sorted data table
    def order_data(self, source_table_name, new_table_name, column, ascending=True):
        if source_table_name not in self.tables:
            print(f"Table '{source_table_name}' does not exist.")
            return f"Table '{source_table_name}' does not exist."
        sorted_data = []
        # For every chunk, we sort the data, and add these data into new table
        for chunk in self.tables[source_table_name]['data']:
            sorted_chunk = sorted(chunk, key=lambda x: x.get(column, None), reverse=not ascending)
            sorted_data.extend(sorted_chunk)
        self.tables[new_table_name] = {'columns': self.tables[source_table_name]['columns'], 'data': [sorted_data]}
        #print(f"data reordered successfully.")
        return f"data reordered successfully."

    # Group the table data and create the new table with new table name to store the grouped data table
    def groupby(self, source_table_name, new_table_name, column):
        # Check if table exist
        if source_table_name not in self.tables:
            print(f"Table '{source_table_name}' does not exist.")
            return f"Table '{source_table_name}' does not exist."
        self.tables[new_table_name] = {'columns': self.tables[source_table_name]['columns'], 'data': []}
        grouped_data = {}
        for chunk in self.tables[source_table_name]['data']:
            for item in chunk:
                key = item.get(column)
                # Create the group column if it does not exist
                if key not in grouped_data:
                    grouped_data[key] = []
                grouped_data[key].append(item)
        for key, items in grouped_data.items():
            self.tables[new_table_name]['data'].append(items)
        return f"data grouped successfully."

    # Using this function to check if value is float
    def safe_check(self, value, default=0.0):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def sum(self, source_table_name, new_table_name, sum_column, groupby_column=None):
        if source_table_name not in self.tables:
            #print(f"Table '{source_table_name}' does not exist.")
            return f"Table '{source_table_name}' does not exist."
        sum_results = {}
        for chunk in self.tables[source_table_name]['data']:
            for item in chunk:
                group_key = item.get(groupby_column) if groupby_column else 'Total'
                sum_value = self.safe_check(item.get(sum_column))
                if group_key not in sum_results:
                    sum_results[group_key] = 0.0
                sum_results[group_key] += sum_value
        columns_definition = {groupby_column: 'str', sum_column: 'float'} if groupby_column else {sum_column: 'float'}
        self.tables[new_table_name] = {'columns': columns_definition, 'data': []}
        for key, value in sum_results.items():
            row_data = {groupby_column: key, sum_column: value} if groupby_column else {sum_column: value}
            self.tables[new_table_name]['data'].append([row_data])
        return f"sum value generated successfully, use show_data new_table to see result"
    
    def count(self, source_table_name, new_table_name, groupby_column=None):
        if source_table_name not in self.tables:
            print(f"Table '{source_table_name}' does not exist.")
            return
        count_results = {}
        for chunk in self.tables[source_table_name]['data']:
            for item in chunk:
                group_key = item.get(groupby_column) if groupby_column else 'Total'
                if group_key not in count_results:
                    count_results[group_key] = 0
                count_results[group_key] += 1
        columns_definition = {groupby_column: 'str', 'Count': 'int'} if groupby_column else {'Count': 'int'}
        self.tables[new_table_name] = {'columns': columns_definition, 'data': []}
        for key, value in count_results.items():
            row_data = {groupby_column: key, 'Count': value} if groupby_column else {'Count': value}
            self.tables[new_table_name]['data'].append([row_data])
        return f"count value generated successfully, use show_data new_table to see result"
    
    def avg(self, source_table_name, new_table_name, avg_column, groupby_column=None):
        if source_table_name not in self.tables:
            print(f"Table '{source_table_name}' does not exist.")
            return
        avg_results = {}
        for chunk in self.tables[source_table_name]['data']:
            for item in chunk:
                group_key = item.get(groupby_column) if groupby_column else 'Total'
                value = self.safe_check(item.get(avg_column))
                if group_key not in avg_results:
                    avg_results[group_key] = {'sum': 0.0, 'count': 0}
                avg_results[group_key]['sum'] += value
                avg_results[group_key]['count'] += 1
        columns_definition = {groupby_column: 'str', 'Average': 'float'} if groupby_column else {'Average': 'float'}
        self.tables[new_table_name] = {'columns': columns_definition, 'data': []}
        for key, values in avg_results.items():
            if values['count'] > 0:
                average = values['sum'] / values['count']
            else:
                average = 0.0
            row_data = {groupby_column: key, 'Average': average} if groupby_column else {'Average': average}
            self.tables[new_table_name]['data'].append([row_data])
        return f"avg value generated successfully, use show_data new_table to see result"
            
    def max(self, source_table_name, new_table_name, max_column, groupby_column=None):
        if source_table_name not in self.tables:
            print(f"Table '{source_table_name}' does not exist.")
            return
        max_results = {}
        for chunk in self.tables[source_table_name]['data']:
            for item in chunk:
                group_key = item.get(groupby_column) if groupby_column else 'Total'
                value = item.get(max_column, None)

                if value is not None:
                    if group_key not in max_results or value > max_results[group_key]:
                        max_results[group_key] = value
        columns_definition = {groupby_column: 'str', max_column: 'float'} if groupby_column else {max_column: 'float'}
        self.tables[new_table_name] = {'columns': columns_definition, 'data': []}
        for key, value in max_results.items():
            row_data = {groupby_column: key, max_column: value} if groupby_column else {max_column: value}
            self.tables[new_table_name]['data'].append([row_data])
        return f"max value generated successfully, use show_data new_table to see result"
            
    def min(self, source_table_name, new_table_name, min_column, groupby_column=None):
        if source_table_name not in self.tables:
            return f"Table '{source_table_name}' does not exist."
        min_results = {}
        for chunk in self.tables[source_table_name]['data']:
            for item in chunk:
                group_key = item.get(groupby_column) if groupby_column else 'Total'
                value = item.get(min_column, None)
                if value is not None:
                    if group_key not in min_results or value < min_results[group_key]:
                        min_results[group_key] = value
        columns_definition = {groupby_column: 'str', min_column: 'float'} if groupby_column else {min_column: 'float'}
        self.tables[new_table_name] = {'columns': columns_definition, 'data': []}
        for key, value in min_results.items():
            row_data = {groupby_column: key, min_column: value} if groupby_column else {min_column: value}
            self.tables[new_table_name]['data'].append([row_data])
        return f"min value generated successfully, use show_data new_table to see result"
    
    def join(self, table1_name, table2_name, new_table_name, table1_join_key, table2_join_key):
        if table1_name not in self.tables or table2_name not in self.tables:
            print(f"One or both tables '{table1_name}' and '{table2_name}' do not exist.")
            return
        self.tables[new_table_name] = {'columns': {**self.tables[table1_name]['columns'], **self.tables[table2_name]['columns']},'data': []}
        for chunk1 in self.tables[table1_name]['data']:
            for item1 in chunk1:
                join_value = item1.get(table1_join_key)
                for chunk2 in self.tables[table2_name]['data']:
                    for item2 in chunk2:
                        if item2.get(table2_join_key) == join_value:
                            joined_record = {**item1, **item2}
                            self.tables[new_table_name]['data'].append([joined_record])
        return f"joined successfully, use show_table new_table to see the result"

    def show_data(self, table_name, limit=None, condition=None, columns=None):
        if table_name not in self.tables:
            return f"Table '{table_name}' does not exist.", False
        data_to_show = []
        for chunk in self.tables[table_name]['data']:
            for row in chunk:
                if condition and not condition(row):
                    continue
                if columns:
                    filtered_row = {col: row.get(col, None) for col in columns}
                    data_to_show.append(filtered_row)
                else:
                    data_to_show.append(row)
                if limit and len(data_to_show) >= limit:
                    break
            if limit and len(data_to_show) >= limit:
                break
        return data_to_show, True
