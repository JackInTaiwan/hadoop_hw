import time
import redis
from timestamp import Timestamp
import pandas as pd
from argparse import ArgumentParser



def insert_bulk(file_path, redis_host, redis_port, redis_db_index):
    global timestamp
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db_index)

    data = pd.read_csv(file_path, delimiter=';')
    print('Load data {} with size of {}.'.format(file_path, data.shape[0]))

    timestamp.stamp('insert')
    
    for i, row in data.iterrows():
        mapping = dict(row)
        del mapping['id']
        client.hmset(row['id'], mapping)
    
    timestamp.stamp('insert')


def update_bulk(file_path, redis_host, redis_port, redis_db_index):
    global timestamp
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db_index)

    data = pd.read_csv(file_path, delimiter=';')
    print('Load data {} with size of {}.'.format(file_path, data.size))

    timestamp.stamp('update')

    new_job, new_sex = 'teacher', 'F'
    for i, row in data.iterrows():
        mapping = {"Mjob": new_job, "sex": new_sex}
        client.hmset(row['id'], mapping)

    timestamp.stamp('update')
    

def search_bulk(file_path, redis_host, redis_port, redis_db_index):
    global timestamp
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db_index, decode_responses=True)

    timestamp.stamp('search')
    sex_filter, paid_filter, internet_filter = 'F', 'no', 'yes'
    output = []

    keys = client.keys('*')
    for key in keys:
        datum = client.hgetall(key)
        if datum['sex'] == sex_filter and datum['paid'] == paid_filter and datum['internet'] == internet_filter:
            output.append(datum)
        
    print('Search task returns {} matched items.'.format(len(output)))

    timestamp.stamp('search')



if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-m', action='store', type=str, required=True, help='mode of manipulation')
    parser = parser.parse_args()

    CSV_FILE_PATH = './student.csv'
    REDIS_HOST, REDIS_PORT, REDIS_DB_INDEX = '127.0.0.1', 6379, 0
    
    timestamp = Timestamp()


    if parser.m == 'insert':
        timestamp.stamp('all')
        
        insert_bulk(CSV_FILE_PATH, REDIS_HOST, REDIS_PORT, REDIS_DB_INDEX)
        
        timestamp.stamp('all')

        print('Total time: {}'.format(timestamp.get_diff('all')))
        print('Insertion time: {}'.format(timestamp.get_diff('insert')))
    

    elif parser.m == 'update':
        timestamp.stamp('all')

        update_bulk(CSV_FILE_PATH, REDIS_HOST, REDIS_PORT, REDIS_DB_INDEX)

        timestamp.stamp('all')

        print('Total time: {}'.format(timestamp.get_diff('all')))
        print('Update time: {}'.format(timestamp.get_diff('update')))


    elif parser.m == 'search':
        timestamp.stamp('all')

        search_bulk(CSV_FILE_PATH, REDIS_HOST, REDIS_PORT, REDIS_DB_INDEX)

        timestamp.stamp('all')

        print('Total time: {}'.format(timestamp.get_diff('all')))
        print('Search time: {}'.format(timestamp.get_diff('search')))