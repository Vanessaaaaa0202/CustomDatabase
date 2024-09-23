# Custom Database System

This project is a custom database system built for **DSCI 551 – Fall 2023**. It supports both relational and NoSQL data models and features a custom query language for interacting with the database through a command-line interface.

## Features

- **Custom Query Language**: A unique query language that allows users to perform operations such as projection, filtering, joining, grouping, aggregation, and ordering.
- **Relational and NoSQL Support**: Supports both relational (SQL-like) and NoSQL (JSON) data storage models.
- **Data Modification Commands**: Includes commands for inserting, updating, and deleting data.
- **Efficient Query Processing**: Handles large datasets without loading the entire database into memory.
- **Command-Line Interface**: An interactive CLI for users to execute database operations.

## File Structure

- **app.py**: Main script for running the command-line interface 
- **cc.py**: Script handling data models and storage (relational and NoSQL) 
- **db1.py**: Script implementing query execution, filtering, joins, etc.

## Installation

To install and run the project, follow these steps:

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/your-repo-name.git

2. Navigate to the project directory:
   ```bash
   cd your-repo-name
   
3. Install required dependencies (if any):
   ```bash
   pip install -r requirements.txt

4. Run the application:
   ```bash
   python app.py

## Query Language Syntax

The custom query language supports the following operations:

- **Selection (Projection):**

  Example:
  ```bash
  MyDB > find employees with name and age;

- **Filtering:**

  Example:
  ```bash
  MyDB > find employees where age >= 30;

- **Join:**

  Example:
  ```bash
  MyDB > find employees and departments on department_id;

And so on!


## Dataset
The project is designed to work with real-world datasets, such as those available from Kaggle or Google Dataset Search. You can load datasets in either CSV or JSON format to test the system.
