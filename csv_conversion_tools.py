import csv
import pandas as pd

def save_dict_as_csv(data, file, **context):
    writer = csv.writer(file, quoting=csv.QUOTE_NONNUMERIC, **context)
    for key, value in data.items():
        writer.writerow([key, value])

def save_dataframe_as_csv(data, file, **context):
    file.write(data.to_csv(**context))

def load_csv_as_json(file, **context):
    return {rows[0]:rows[1] for rows in csv.reader(file, quoting=csv.QUOTE_NONNUMERIC, **context)}

def load_csv_as_dataframe(file, **context):
    return pd.read_csv(file, **context)
