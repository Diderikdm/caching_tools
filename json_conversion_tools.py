import json
import pandas as pd


def save_dict_as_json(data, file, **context):
    print(data)
    json.dump(data, file, ensure_ascii=False, indent=4, **context)

def save_dataframe_as_json(data, file, **context):
    json.dump(data.to_json(**context), file, ensure_ascii=False, indent=4)

def load_json_as_json(file, **context):
    object = json.load(file, **context)
    if type(object) == dict:
        return object
    return json.loads(object, **context)

def load_json_as_dataframe(file, **context):
    return pd.DataFrame.from_dict(load_json_as_json(file), **context)
