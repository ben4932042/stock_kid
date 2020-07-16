from google.cloud import storage
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_news')

blob = bucket.get_blob('1101/2020-06-10_4490566.json')
Dic = eval(blob.download_as_string().decode('utf-8'))


