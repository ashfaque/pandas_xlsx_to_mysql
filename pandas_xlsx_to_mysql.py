import os
import pandas as pd
import sqlalchemy
from urllib.parse import quote_plus

# print(pd.__version__)

df = pd.read_excel(f'{os.getcwd()}\\Lifequotes.xlsx', engine='openpyxl', sheet_name='Sheet1', usecols=['Quotes', 'By'])

df.reset_index(drop = True, inplace=True)

df.rename(columns = {'Quotes':'quote', 'By':'author'}, inplace=True)

from datetime import datetime
df.insert(loc=len(df.columns), column='is_read', value=0)    # ? Insert at the last position.
df.insert(loc=len(df.columns), column='is_deleted', value=0)    # ? Insert at the last position.
df.insert(loc=len(df.columns), column='created_at', value=datetime.now())    # ? Insert at the last position.
df.insert(loc=len(df.columns), column='created_by_id', value=1)    # ? Insert at the last position.

database_username = 'username'
database_password = 'password@123'
database_ip       = '192.168.0.100'
database_port     = 3306
database_name     = 'db_name'

engine = sqlalchemy.create_engine(f'mysql+mysqlconnector://{database_username}:{quote_plus(database_password)}@{database_ip}:{database_port}/{database_name}', echo=False)

df.to_sql(name="table_name_in_db", con=engine, if_exists="append", index=False)

engine.dispose()



for index, row in df.iterrows():
    insert_data = {}
    insert_data['quote'] = row['quote'].strip()
    insert_data['author'] = row['author'].strip()
    print(insert_data)
