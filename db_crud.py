import pymysql
import sqlalchemy

connection_url = 'mysql+pymysql://root:password@#.##.###.###:####'


def create_event(user_id, car_id, path_original):
    query = sqlalchemy.text("INSERT INTO event (user_id, car_id, path_original) VALUES ({}, {}, {});".format(user_id, car_id, "\'" + path_original + "\'"))
    db = sqlalchemy.create_engine(connection_url)
    try:
        with db.connect() as conn:
            conn.execute(query)
    except Exception as e:
        print('Error: {}'.format(str(e)))
    print('Success')

def select_event(path_original):
    query = sqlalchemy.text("SELECT * FROM event WHERE path_original={};".format("\'" + path_original + "\'"))
    db = sqlalchemy.create_engine(connection_url)
    try:
        with db.connect() as conn:
            result = conn.execute(query)
            result = result.first()
    except Exception as e:
        print('Error: {}'.format(str(e)))
    print('Success')
    return result

def update_event(path_original, is_damaged, conf_score=None):
    event_id = select_event(path_original).id
    if conf_score:
        query = sqlalchemy.text("UPDATE event SET is_damaged_1={}, is_damaged_2={}, is_damaged_3={}, is_damaged_4={}, is_damaged_5={}, is_damaged_6={}, conf_score={} WHERE id={};".format(is_damaged[0], is_damaged[1], is_damaged[2], is_damaged[3], is_damaged[4], is_damaged[5] , conf_score, event_id))
    db = sqlalchemy.create_engine(connection_url)
    try:
        with db.connect() as conn:
            conn.execute(query)
    except Exception as e:
        print('Error: {}'.format(str(e)))
    print('Success')


def create_entry(path_original, is_inferenced, is_inspected):
    event_id = select_event(path_original).id
    query = sqlalchemy.text("INSERT INTO entry (event_id, is_inferenced, path_inference_dent, path_inference_scratch, path_inference_spacing, is_inspected) VALUES ({0}, {1}, {2}, {2}, {2}, {3});".format(event_id, is_inferenced, "\'" + path_original + "\'", is_inspected))
    db = sqlalchemy.create_engine(connection_url)
    try:
        with db.connect() as conn:
            conn.execute(query)
    except Exception as e:
        print('Error: {}'.format(str(e)))
    print('Success')
