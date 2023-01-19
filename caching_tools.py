from pathlib import Path
import os
import logging
import pandas as pd
from caching_tools.json_conversion_tools import *
from caching_tools.csv_conversion_tools import *
from caching_tools.pickle_conversion_tools import *

CACHE_LOCATION = ["gcs", "data", "cache"]

SAVE_OBJECT_AS_TYPE = {
    # { From python type : { To cache datatype : logic ... } ... }
    dict            : {
        "json"          :   save_dict_as_json,
        "pickle"        :   save_as_pickle,
        "csv"           :   save_dict_as_csv,
    },
    pd.DataFrame    : {
        "json"          :   save_dataframe_as_json,
        "pickle"        :   save_as_pickle,
        "csv"           :   save_dataframe_as_csv,
    },
    list            : {
        "pickle"        :   save_as_pickle
    },
    str             : {
        "json"          :   save_dict_as_json,
    },

}


CONVERT_CACHED_OBJECT_TO_TYPE = {
    # { From cached datatype : { To python datatype : logic ... } ...}
    "json"          : {
        "json"          :   load_json_as_json,
        "dataframe"     :   load_json_as_dataframe,
    },
    "pickle"        : {
        "json"          :   load_pickle_as_json,
        "dataframe"     :   load_pickle_as_dataframe,
        "list"          :   load_pickle
    },
    "csv"           : {
        "json"          :   load_csv_as_json,
        "dataframe"     :   load_csv_as_dataframe,
    }
}

READ_TYPE_PER_CONVERSION = {
    # {convert-to type : read-type}
    "json"      :   "r",
    "pickle"    :   "rb",
    "csv"       :   "r"
}

WRITE_TYPE_PER_CONVERSION = {
    # {convert-to type : write-type}
    "json"      :   "w",
    "pickle"    :   "wb",
    "csv"       :   "w"
}


def path_builder(*args, path=Path.home(), filename=''):
    """
    Use:
        Build a custom path to a file
    Args:
        path (optional) --> homepath to start the path from (default to system homepath) (str)
        args (optional) --> any consequal arguments (subfolders) to append to the path (list/tuple)
        filename (optional) --> specify a filename (WITH EXTENTION) to append to the path for the full filepath. Leave empty to return folder path.
    Returns:
        Custom filepath
    """
    return Path(f"{'/'.join([str(path)] + [*args])}/{filename}")


def get_path_for_airflow_cache(subfolders=[], filename=''):
    """
    Use:
        Build the path to the cache (with optional subpath / filename to extend the path with)
    Args:
        subfolders (optional) --> any consequal arguments (subfolders) to append to the path (list/tuple)
        filename (optional) --> specify a filename (WITH EXTENTION) to append to the path for the full filepath. Leave empty to return folder path.
    Returns:
        (custom extended) filepath to the cache
    """
    return path_builder(*CACHE_LOCATION, *subfolders, filename=filename)

    
def validate_or_alternate_filename(filename, data_type):
    """
    Use:
        Function to try to validate the current filename extention type (if any) and if needed convert this to the specified data_type
    Args:
        filename --> name of the file (preferably with extention) (str)
        data_type --> specify the datatype to compare/overwrite the filename with (str)
    Returns:
        validated or altered filename
    """
    altered_filename = None
    if not filename.endswith(f".{data_type}"):
        altered_filename = f"{filename[:filename.rfind('.')] if '.' in filename else filename}.{data_type}"
        logging.warning(f"filename '{filename}' does not match the current data type '{data_type}'. altering filename to '{altered_filename}'")
    return altered_filename or filename


def convert_data_package_to_cache_type_and_save(data, data_type, file, **context):
    """
    Use:
        Function to convert python type into other type and save the contents in a given file. 
        Is limited to the configuration of SAVE_OBJECT_AS_TYPE. 
    Args:
        data --> payload -> The actual object to be converted in it's original state. (object)
        data_type --> specify the datatype to convert the python object into. (str)
        file --> file to write the converted data into. (file)
        context (optional) --> keyword arguments as required by specific filetypes (any)
    Returns:
        None
    """
    try:
        print(f"context: {context}, {type(context)}")
        print(type(data))
        return SAVE_OBJECT_AS_TYPE.get(type(data)).get(data_type.lower())(data, file, **context)
    except TypeError as e:
        print(e)
        logging.warn(f"""Method for saving ----> {type(data)} <---- objects in type format ----> {data_type} <---- has not yet been (correctly) defined. 
        Please do so in caching_tools.SAVE_OBJECT_AS_TYPE""")
        raise e


def get_write_type(save_as_data_type):
    """
    Use:
        Retrieve the correct file-write type for saving files as certain datatypes
    Args:
        save_as_data_type --> type to save the file as (str)
    Returns:
        write type for the current data type (str)
    """
    try:
        return_type = WRITE_TYPE_PER_CONVERSION.get(save_as_data_type)
        if not return_type:
            raise TypeError
        return return_type
    except TypeError as e:
        logging.warn(f"""Write type for saving in type format ----> {save_as_data_type} <---- has not yet been (correctly) defined. 
        Please do so in caching_tools.WRITE_TYPE_PER_CONVERSION""")
        raise e


def save_file_in_cache(filename, data, subfolders=[], save_as_data_type="json", generate_nonexistant_folders=True, direct_path=None, **context):
    """
    Use:
        Saves a python object to the Google Storage folder serving as cache memory. 
        This function can be parameterized to convert into any pre-specified cache-type fileformat.
    Args:
        filename --> Name of the to-be-cached file (str)
        data --> payload -> The actual object to be cached in it's original state. (object)
        subfolders (optional) --> specify a list of folders rooted from the cache-folder to location you want to save (list(str))
        save_as_data_type (optional) --> specify the type you want to convert the python type data into. Default is json. (str)
        generate_nonexistant_folders (optional) --> This flag will (not) create non-existant subfolders rooted from the home path (default is True) (boolean)
        direct_path (optional) --> In cases where the parent directory of the filename is not the one used in 
                        the production bucket. (str)
        context (optional) --> keyword arguments as required by specific conversions (any)
    Returns:
        None, but saves the object as given type in the path specified.
    """
    if not direct_path:
        file_path = get_path_for_airflow_cache(subfolders=subfolders, filename=validate_or_alternate_filename(filename, save_as_data_type))
    else:
        file_path = path_builder(path=direct_path, filename=validate_or_alternate_filename(filename, save_as_data_type))
    if generate_nonexistant_folders:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, get_write_type(save_as_data_type)) as cache_file:
        print(f"\nSaving {data} as {save_as_data_type} in {file_path}\n")
        convert_data_package_to_cache_type_and_save(data, save_as_data_type, cache_file, **context)


def convert_type_from_cache_to_python(cache_file, convert_to_type, cached_datatype, **context):
    """
    Use:
        Function to convert cached type object into python type and save the contents in an object. 
        Is limited to the configuration of CONVERT_CACHED_OBJECT_TO_TYPE. 
    Args:
        cache_file --> file to read the data from. (file)
        convert_to_type --> specify the datatype to convert the cached object into. (str)
        cached_datatype --> specify the current datatype of the cached object. (str)
        context (optional) --> keyword arguments as required by specific filetypes (any)
    Returns:
        converted python type (object)
    """
    try:
        print(f"context: {context}, {type(context)}")
        return CONVERT_CACHED_OBJECT_TO_TYPE.get(cached_datatype.lower()).get(convert_to_type.lower())(cache_file, **context)
    except TypeError as e:
        print(e)
        logging.warn(f"""Method for retrieving objects in cached type format ----> {cached_datatype} <---- to converted python type 
        ----> {convert_to_type} <---- has not yet been (correctly) defined. Please do so in caching_tools.CONVERT_CACHED_OBJECT_TO_TYPE""")
        raise e


def get_read_type(cached_datatype):
    """
    Use:
        Retrieve the correct file-read type for cached files
    Args:
        cached_datatype --> type of the cached file (str)
    Returns:
        read type for the cached data type (str)
    """
    try:
        return_type = READ_TYPE_PER_CONVERSION.get(cached_datatype)
        if not return_type:
            raise TypeError
        return return_type
    except TypeError as e:
        logging.warn(f"""Read type for saving in type format ----> {cached_datatype} <---- has not yet been (correctly) defined. 
        Please do so in caching_tools.READ_TYPE_PER_CONVERSION""")
        raise e


def get_file_from_cache(filename, subfolders=[], convert_to_type="json", cached_datatype="json", direct_path=None, **context):
    """
    Use:
        Gets file from Google Storage folder serving as cache memory.
        Some sheet names have spaces so in these cases looking for the exact name in the cache
        causes the code to fail. We use a fallback to bypass such issues.
    Args:
        filename --> Name of the sheet (str)
        subfolders (optional) --> specify a list of folders rooted from the cache-folder to the file you want to access (list(str))
        convert_to_type (optional) --> specify the type you want to convert the cached data into. Default is json. (str)
        cached_datatype (optional) --> specify the type of the file you want to access in the cache. Default is json. 
                                       Note that this flag overrides any extention passed as an argument inside the filename. (str)
        direct_path (optional) --> In cases where the parent directory of the filename is not the one used in 
                        the production bucket. (str)
        context (optional) --> keyword arguments as required by specific conversions (any)
    Returns:
        python object as specified by convert_to_type. (object)
    """
    if not direct_path:
        cache_file_path = get_path_for_airflow_cache(subfolders=subfolders, filename=validate_or_alternate_filename(filename, cached_datatype))
    else:
        cache_file_path = path_builder(path=direct_path, filename=validate_or_alternate_filename(filename, cached_datatype))
        
    if not cache_file_path.exists():
        logging.warn(f"File {validate_or_alternate_filename(filename, cached_datatype)} does not exist in the cache. Retrying without whitespace.")
        stripped_filename = filename.replace(' ','')
        cache_file_path = get_path_for_airflow_cache(subfolders=subfolders, filename=validate_or_alternate_filename(stripped_filename, cached_datatype))
        if not cache_file_path.exists():
            raise Exception(f"""Files [{validate_or_alternate_filename(filename, cached_datatype)} and 
            {validate_or_alternate_filename(stripped_filename, cached_datatype)}] don't exist in {get_path_for_airflow_cache(subfolders)}""")
        filename = stripped_filename
    
    with open(cache_file_path, get_read_type(cached_datatype)) as cache_file:
        print(f"\nLoading {filename} from {cache_file} from type {cached_datatype} to {convert_to_type}\n")
        return convert_type_from_cache_to_python(cache_file, convert_to_type, cached_datatype, **context)
