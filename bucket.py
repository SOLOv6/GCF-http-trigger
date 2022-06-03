from google.cloud import storage


def get_bucket(bucket_name, prefix, folder_name, json_key):
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