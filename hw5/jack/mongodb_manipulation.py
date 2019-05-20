import pymongo
import pandas as pd
from argparse import ArgumentParser
from timestamp import Timestamp
from pymongo import MongoClient



def insert_bulk(file_path, db_host, db_port, db_name, db_col_name):
    client = MongoClient(db_host, db_port)
    
    db = client[db_name]
    collection = db[db_col_name]
    
    data = pd.read_csv(file_path, delimiter=';')
    print('Load data {} with size of {}.'.format(file_path, data.shape[0]))

    timestamp.stamp('insert')
    
    bulk = []
    for i, row in data.iterrows():
        datum = dict(row)
        bulk.append(datum)

    collection.insert_many(bulk)

    timestamp.stamp('insert')


def update_bulk(file_path, db_host, db_port, db_name, db_col_name):
    client = MongoClient(db_host, db_port)
    
    db = client[db_name]
    collection = db[db_col_name]

    global timestamp
    timestamp.stamp('update')

    new_job, new_sex = 'teacher', 'F'
    res = collection.update_many({}, {'$set': {'Mjob': new_job, 'sex': new_sex}})

    timestamp.stamp('update')


def search_bulk(file_path, db_host, db_port, db_name, db_col_name):
    client = MongoClient(db_host, db_port)
    
    db = client[db_name]
    collection = db[db_col_name]

    global timestamp
    timestamp.stamp('search')

    sex_filter, paid_filter, internet_filter = 'F', 'no', 'yes'
    cursor = collection.find({'sex': sex_filter, 'paid': paid_filter, 'internet': internet_filter})
    output = list(cursor)
    
    print('Search task returns {} matched items.'.format(len(output)))

    timestamp.stamp('search')


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-m', action='store', type=str, required=True, help='mode of manipulation')
    parser = parser.parse_args()

    CSV_FILE_PATH = './student.csv'
    MONGO_HOST, MONGO_PORT, MONGO_DB_NAME, MONGO_COL_NAME = '127.0.0.1', 27017, 'demoDb', 'student'
    
    timestamp = Timestamp()


    if parser.m == 'insert':
        timestamp.stamp('all')
        
        insert_bulk(CSV_FILE_PATH, MONGO_HOST, MONGO_PORT, MONGO_DB_NAME, MONGO_COL_NAME)
        
        timestamp.stamp('all')

        print('Total time: {}'.format(timestamp.get_diff('all')))
        print('Insertion time: {}'.format(timestamp.get_diff('insert')))
    

    elif parser.m == 'update':
        timestamp.stamp('all')

        update_bulk(CSV_FILE_PATH, MONGO_HOST, MONGO_PORT, MONGO_DB_NAME, MONGO_COL_NAME)

        timestamp.stamp('all')

        print('Total time: {}'.format(timestamp.get_diff('all')))
        print('Update time: {}'.format(timestamp.get_diff('update')))


    elif parser.m == 'search':
        timestamp.stamp('all')

        search_bulk(CSV_FILE_PATH, MONGO_HOST, MONGO_PORT, MONGO_DB_NAME, MONGO_COL_NAME)

        timestamp.stamp('all')

        print('Total time: {}'.format(timestamp.get_diff('all')))
        print('Search time: {}'.format(timestamp.get_diff('search')))