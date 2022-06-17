import requests
import base64
import io
import json
from bucket import get_bucket
from db_crud import create_entry, create_event, update_event, select_event

connection_url = 'mysql+pymysql://root:password@#.##.###.####:####'


JSON = "key.json"
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
        res = requests.post("http://##.###.###.###:####/predictions/model", data=data)
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


    return '-------FINAL SUCCESS-------'
