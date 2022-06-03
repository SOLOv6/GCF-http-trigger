from google.cloud import storage
import requests
import base64
import io
import json
import pymysql
import sqlalchemy
import os

connection_url = 'mysql+pymysql://root:password@3.39.180.133:3306/solodb'

JSON = "service_key.json"
BUCKET_NAME = "solov6-test-storage"


def gcs_trigger(request):

    request_json = request.get_json()
    img_path_name = request_json["img_path"] 

    prefix = f"path_original/{img_path_name}"
    folder = "path_original"

    print(f"PREFIX: {PREFIX}")

    # download images from buccket
    img_path, serving = get_bucket(BUCKET_NAME, prefix, folder, JSON)

    #is_damaged = False
    is_damaged = []
    # infereced
    result = 0.
    for serve, path in zip(serving, img_path):
        data = {"data": serve, "path": path}
        res = requests.post("http://34.133.150.214:3000/predictions/model", data=data)
        print(f"STATUS: {res.status_code}")
        res = res.content.decode()
        res = io.StringIO(res)
        res = json.load(res)
        if result <= res["conf"]:
            result = res["conf"]

        print(res["is_damaged"])   
        if res["is_damaged"] == "true":
            print("----------DAMAGED----------")
            is_damaged.append(True)
        else:
            is_damaged.append(False)
        
    print(f"RESULT: {result}")
    print(f"PATH: {img_path}")

    path_wop = [img.split(".")[0] for img in img_path]
    all_path = "_".join(path_wop) + "_"

    year, mon, days, user, car, _ = img_path[0].split("-")
    print(f"ALL_PATH : {all_path}")

    # DB query
    create_event(user, car, all_path)
    update_event(all_path, is_damaged, result)
    create_entry(all_path, is_inferenced=True, is_inspected=False)

    print("-------FINAL SUCCESS-------")

    return 'shiiittttt!!!!!!!'


def get_bucket(bucket_name, prefix, folder_name, json_key = JSON):
    img_path = []
    serving = []
    
    storage_client = storage.Client()
    storage_client.from_service_account_json(json_key)
    
    bucket = storage_client.bucket(bucket_name)
    
    blobs = bucket.list_blobs(prefix = prefix)
    for blob in blobs:
        filename = blob.name.split("/")[1]
        img_path.append(filename)
        print(filename)
        print(folder_name +"/"+ filename)
        blob = bucket.blob(folder_name +"/"+ filename)
        print(blob)
        img = blob.download_as_bytes()
        serving.append(base64.b64encode(img))

    return img_path, serving


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
