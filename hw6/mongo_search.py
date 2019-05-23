import pymongo
from argparse import ArgumentParser
from timestamp import Timestamp
from pymongo import MongoClient, UpdateMany, UpdateOne



def insert_bulk(file_path, db_host, db_port, db_name, db_col_name, task_index):
    client = MongoClient(db_host, db_port)
    db = client[db_name]
    collection = db[db_col_name]

    timestamp.stamp('insert')

    if task_index == 0:
        collection.create_index('S')
        bulk = []
        
        with open(file_path) as f:
            for line in f.readlines():
                doc = line.strip().split(' ')
                doc = dict([(k, v) for (k, v) in zip(['S', 'P', 'O'], doc)])
                bulk.append(doc)
        
        collection.insert_many(bulk)

    elif task_index == 1:
        collection.create_index('O')
        bulk = []
        
        with open(file_path) as f:
            for line in f.readlines():
                doc = line.strip().split(' ')
                doc = dict([(k, v) for (k, v) in zip(['S', 'P', 'O'], doc)])
                bulk.append(doc)
        
        collection.insert_many(bulk)

    elif task_index == 2:
        collection.create_index('S', unique=True)
        collection.create_index('P_list')
        bulk = []
        
        with open(file_path) as f:
            for line in f.readlines():
                doc = line.strip().split(' ')
                bulk.append(UpdateMany({'S': doc[0]}, {'$addToSet': {'P_list': doc[1]}}, upsert=True))
        
        collection.bulk_write(bulk)

    elif task_index == 3:
        collection.create_index('O')
        # create compound index 
        collection.create_index([('O', pymongo.ASCENDING), ('S', pymongo.ASCENDING)], unique=True)
        bulk = []

        with open(file_path) as f:
            for line in f.readlines():
                doc = line.strip().split(' ')
                bulk.append(UpdateOne({'O': doc[2], 'S': doc[0]}, {'$inc': {'count': 1}}, upsert=True))
        
        collection.bulk_write(bulk)

    timestamp.stamp('insert')


def search_0(db_host, db_port, db_name, db_col_name, task_index):
    global timestamp
    client = MongoClient(db_host, db_port)
    db = client[db_name]
    collection = db[db_col_name]

    si_list = ['Shamsuzzaman_Khan', 'Jake_Gyllenhaal', 'Rubn_Arocha', 'frBgnet', 'Cody_Kessler']

    timestamp.stamp('search')

    for si in si_list:
        cursor = collection.find({'S': si})
        res = [(_doc['P'], _doc['O']) for _doc in cursor] 
        print('Search: {} | Number of results: {} | {}'.format(si, len(res), res))

    timestamp.stamp('search')


def search_1(db_host, db_port, db_name, db_col_name, task_index):
    global timestamp
    client = MongoClient(db_host, db_port)
    db = client[db_name]
    collection = db[db_col_name]

    oi_list = ['France', 'Belgium', 'Canada', 'Scotland', 'Spain']

    timestamp.stamp('search')

    for oi in oi_list:
        cursor = collection.find({'O': oi})
        res = [(_doc['S'], _doc['P']) for _doc in cursor] 
        print('Search: {} | Number of results: {}'.format(oi, len(res)))

    timestamp.stamp('search')


def search_2(db_host, db_port, db_name, db_col_name, task_index):
    global timestamp
    client = MongoClient(db_host, db_port)
    db = client[db_name]
    collection = db[db_col_name]

    p_pair = ('isLeaderOf', 'isCitizenOf')

    timestamp.stamp('search')

    cursor = collection.find({'P_list': {'$all': list(p_pair)}})
    res = [_doc['S'] for _doc in cursor] 
    print('Search: {} | Number of results: {} \n| Partial Samples: {}'.format(p_pair, len(res), res[:5]))

    timestamp.stamp('search')


def search_3(db_host, db_port, db_name, db_col_name, task_index):
    global timestamp
    client = MongoClient(db_host, db_port)
    db = client[db_name]
    collection = db[db_col_name]

    oi_list = ['DurhamSud', 'Norway', 'Germany', 'France', 'China']

    timestamp.stamp('search')
    
    for oi in oi_list:
        cursor = collection.aggregate([
            {'$match': {'O': oi}},
            {'$sort': {'count':-1}},
            {'$limit': 1},
        ])

        res = list(cursor)[0]['S']
        print('Search: {} | Result: {}'.format(oi, res))

    timestamp.stamp('search')


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-m', action='store', type=str, required=True, help='mode [insert, task]')
    parser.add_argument('-i', action='store', type=int, required=True, help='index of task')
    parser = parser.parse_args()

    FILE_PATH = './yagoThreeSimplifiedShort.txt'
    MONGO_HOST, MONGO_PORT, MONGO_DB_NAME, MONGO_COL_NAME = '127.0.0.1', 27017, 'demoDb', 'yago_{}'.format(parser.i)
    
    timestamp = Timestamp()


    if parser.m == 'insert':
        timestamp.stamp('all')
        
        insert_bulk(FILE_PATH, MONGO_HOST, MONGO_PORT, MONGO_DB_NAME, MONGO_COL_NAME, parser.i)
        
        timestamp.stamp('all')

        print('Total time: {}'.format(timestamp.get_diff('all')))
    

    elif parser.m == 'search':
        timestamp.stamp('all')
        
        if parser.i == 0:
            search_0(MONGO_HOST, MONGO_PORT, MONGO_DB_NAME, MONGO_COL_NAME, parser.i)

        elif parser.i == 1:
            search_1(MONGO_HOST, MONGO_PORT, MONGO_DB_NAME, MONGO_COL_NAME, parser.i)

        elif parser.i == 2:
            search_2(MONGO_HOST, MONGO_PORT, MONGO_DB_NAME, MONGO_COL_NAME, parser.i)

        elif parser.i == 3:
            search_3(MONGO_HOST, MONGO_PORT, MONGO_DB_NAME, MONGO_COL_NAME, parser.i)
        
        timestamp.stamp('all')

        print('Total time: {}'.format(timestamp.get_diff('all')))
        print('Search time: {}'.format(timestamp.get_diff('search')))