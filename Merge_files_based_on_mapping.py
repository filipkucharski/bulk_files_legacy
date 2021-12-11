import os
import glob
import pandas as pd
import re
import sys
import argparse

#read std mapping and create config dict
def convert_std_to_dict(std_path):
    df = pd.read_csv(std_path, sep=",").fillna("IGNORE")
    s = df.set_index(df.columns[0]).T.to_dict('series')
    return s			
#get db_name and dimension of each file we want to use
def get_db_name_and_dimension(prod_path,regex_pattern):
    db_name_search = re.search(regex_pattern,prod_path)
    db_name = db_name_search.group(1)
    return db_name
#iterate through the files and create final file
def main(s,db_name,encoding):
    #create list of col to load for a db
    try:
        list_col = s[db_name].values.tolist()
    except:
        print(f"{db_name} not included in std table")
        return
    list_col = [v for v in list_col if v != "IGNORE"]
    try:
        df_current = pd.read_csv(prod_path,usecols = list_col,dtype=str,encoding=encoding)
        df_current = df_current[list_col]
        list_index_all = s[db_name].index
        list_index = [v for v in list_index_all if s[db_name].loc[v] != "IGNORE"]
        #headings missing from specific db
        list_index_none = [v for v in list_index_all if s[db_name].loc[v] == "IGNORE"]
        #rename loaded columns for db
        df_current.columns = list_index
        #add column with db_name
        df_current["db_name"] = db_name
        #add empty columns
        df_current[list_index_none] = ""
        #order final columns as in the std table
        columns_final = ["db_name"] + list_index_all.tolist()
        df_current = df_current[columns_final]
    except ValueError as e:
        print(f"Wrong columns provided for {db_name}")
        print(e)
        return
    except UnicodeDecodeError as e:
        print(f"Encoding problem for {db_name}")
        print(e)
        return
    return df_current

if __name__ == "__main__":

    # Initialize argument parsing
    parser = argparse.ArgumentParser(description = "Merge files based on mapping table")
    parser.add_argument("--directory", help = "Path of the directory where the files are", type=str)
    parser.add_argument("--dimension", help = "Which dimension to look for")
    parser.add_argument("--std_path", help = "Path to the mapping table",type=str)
    parser.add_argument("--regex_pattern", help = "Regex to get DB name from the path of each file",type=str)
    parser.add_argument("--encoding", help = "Encoding to be used to read the files")

    args = parser.parse_args()

    prod_paths_all = glob.glob(os.path.join(args.directory,"*.csv"))
    prod_paths = []
    for file in prod_paths_all:
        if args.dimension in file:
            prod_paths.append(file)
    print(f"Those files will be merged:{prod_paths}")
    s = convert_std_to_dict(args.std_path)
    df = pd.DataFrame()

    for file in prod_paths:
        prod_path = file
        db_name = get_db_name_and_dimension(prod_path,args.regex_pattern)
        df_current = main(s,db_name,args.encoding)
        df = pd.concat([df,df_current],axis=0)

    df.to_csv(f"{args.dimension}_files_merged.csv",index=False,encoding="UTF-8")
    print("All done")