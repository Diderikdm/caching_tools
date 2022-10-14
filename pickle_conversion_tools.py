import pickle 
import pandas as pd

def save_as_pickle(data, file, **context): 
    print("saving as pkl")
    pickle.dump(data, file, **context)

def load_pickle_as_json(file, **context):
    object = pickle.load(file, **context)
    if type(object) == pd.DataFrame:
        return object.to_json(**context)
    elif type(object) == dict:
        return object

def load_pickle_as_dataframe(file, **context):
    object = pickle.load(file, **context)
    if type(object) == dict:
        return pd.DataFrame.from_dict(object, **context)
    elif type(object) == pd.DataFrame:
        return object

def load_pickle(file, **context):
    return pickle.load(file, **context)
