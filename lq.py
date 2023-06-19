from sqlalchemy import (
    create_engine,
    MetaData,
)
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
import pdb
import os
from random import randint
import threading
from copy import copy

# local paths
BASE_PATH = os.path.abspath(os.curdir)

postgresql="postgresql://postgres:onlylocalpass@localhost:5432/lq_feed"
postgresql_aws1 = "postgresql://postgres:onlylocalpassword@***.us-west-1.rds.amazonaws.com:5432/lq_feed"
aws_access_key_id=""
aws_secret_access_key=""

# set up engine for database
Base = declarative_base()
metadata = MetaData()
engine = create_engine(postgresql_aws1)

# random intervals
INPUT_INTERVAL = 2
COLUMN_INTERVAL = 3
WATCH_INTERVAL = 2

class Feed:
    random_tb_name = 'random_input'
    report_tb_name = 'report'
    columns = ['col1', 'col2', 'col3', 'col4']
    prev_columns = []
    new_columns = []
    data_to_insert = []

    def __init__(self):
        print("[***] connect db")
        Base.metadata.create_all(engine)

        self.create_random_table()
        self.create_report_table()

        self.read_existing_columns_from_table()
        self.prev_columns = copy(self.columns)

    def create_random_table(self):
        with Session(engine) as s:
            s.execute(text(f'CREATE TABLE IF NOT EXISTS {self.random_tb_name} ( \
                        id SERIAL PRIMARY KEY, \
                        col1 character varying , \
                        col2 character varying , \
                        col3 character varying , \
                        col4 character varying,  \
                        created_at timestamp without time zone NOT NULL DEFAULT now() \
            )'))
            s.commit()

    def create_report_table(self):
        with Session(engine) as s:
            s.execute(text(f'CREATE TABLE IF NOT EXISTS {self.report_tb_name} ( \
                        id SERIAL PRIMARY KEY, \
                        content text,  \
                        added_columns text, \
                        removed_columns text, \
                        created_at timestamp without time zone NOT NULL DEFAULT now() \
            )'))
            s.commit()

    def add_new_columns_to_table(self):
        print(f'[add_new_columns] add {len(self.new_columns)} new column to table {self.random_tb_name}')
        s_columns = []
        for col in self.new_columns:
            s_columns.append(f"ADD COLUMN {col} character varying")
        s_col_text = ', '.join(s_columns)

        with Session(engine) as s:
            try:
                s.execute(text(f'ALTER TABLE {self.random_tb_name} \
                        {s_col_text} \
                '))
                s.commit()
            except Exception as err:
                print('[*Error*] [add_new_columns_to_table] ', err)

    def insert_data(self, tb_name, cols, data):
        print(f'[insert_data] insert data into the table {tb_name}')
        s_cols = ', '.join(cols)
        s_vals = []
        for col in cols:
            s_vals.append(f':{col}')
        with Session(engine) as s:
            try:
                raw_query = f"""INSERT INTO {tb_name}({s_cols}) VALUES ({', '.join(s_vals)})"""
                s.execute(text(raw_query), data)
                s.commit()
            except Exception as error:
                print('[*Error*] [insert_data] ', error)

    def read_existing_columns_from_table(self):
        # print(f'[read_existing_columns_from_table] in table {self.random_tb_name}')
        with engine.connect() as con:
            cols = con.execute(text(f"SELECT column_name \
                    FROM information_schema.columns \
                    WHERE table_schema = 'public' AND table_name = '{self.random_tb_name}'; \
            "))
            functional_columns = ['id', 'created_at', 'updated_at']
            for col in cols:
                _col = col[0]
                if _col not in functional_columns:
                    if _col not in self.columns:
                        self.columns.append(_col)
    
    def generate_random_columns(self):
        self.read_existing_columns_from_table()

        while True:
            r_column = f"col{randint(0, 50)}"
            if r_column not in self.columns:
                self.new_columns = [r_column]
                break
    
    def generate_random_input(self):
        self.read_existing_columns_from_table()
        r_data = {}
        for col in self.columns:
            r_data[col] = f"{randint(0, 10)}"
        
        self.data_to_insert = r_data

    def insert_random_data(self):
        self.insert_data(self.random_tb_name, self.columns, self.data_to_insert)
        
    def build_random_table(self):
        self.generate_random_input()
        self.insert_random_data()

    # add random data to the table every 2 seconds
    def manage_random_data(self):
        threading.Timer(INPUT_INTERVAL, self.manage_random_data).start()
        self.build_random_table()

    # add a new column every 10 seconds
    def add_columns_randomly(self):
        threading.Timer(COLUMN_INTERVAL, self.add_columns_randomly).start()
        self.generate_random_columns()
        self.add_new_columns_to_table()

    def detect_column_change(self):
        self.read_existing_columns_from_table()
        removed_columns = []
        for element in self.prev_columns:
            if element not in self.columns:
                removed_columns.append(element)

        added_columns = []
        for element in self.columns:
            if element not in self.prev_columns:
                added_columns.append(element)

        if added_columns or removed_columns:
            # columns changed. report it
            report_data = { 'content': ''}
            report_columns = []
            if added_columns:
                report_data['content'] += f'{len(added_columns)} columns added, '
                report_data['added_columns'] = ', '.join(added_columns)
                report_columns.append('added_columns')
            if removed_columns:
                report_data['content'] += f'{len(removed_columns)} columns removed'
                report_data['removed_columns'] = '"' +  '.join(removed_columns)' + '"'
                report_columns.append('removed_columns')

            report_columns.append('content')
            print(f'[detect column] {report_data["content"]}')
            self.insert_data(self.report_tb_name, report_columns, report_data)
            self.prev_columns = copy(self.columns)

    # watch the input table in terms of column change
    def watch_table(self):
        threading.Timer(WATCH_INTERVAL, self.watch_table).start()
        self.detect_column_change()

    def run(self):
        self.manage_random_data()

        self.add_columns_randomly()

        self.watch_table()

if __name__ == "__main__":
    feed = Feed()
    feed.run()
