"""
Utitlity functions to work with data on https://github.com/cbioPortal/datahub directly
"""
import pandas as pd
import numpy as np
import glob
import os
from typing import Literal
from tqdm import tqdm
import pyarrow.parquet as pq
import pyarrow as pa


def get_remote_maf_df(study_id):
    return pd.read_csv("https://media.githubusercontent.com/media/cBioPortal/datahub/master/public/{}/data_mutations.txt".format(study_id),skiprows=2,sep="\t")

def get_local_all_combined_data_from_folders(folder):
    return {
        "maf":get_local_combined_data_from_folders(folder, 'mutations'),
        "clinp":get_local_combined_data_from_folders(folder, 'patient'),
        "clins":get_local_combined_data_from_folders(folder, 'sample'),
    }

def get_local_combined_data_from_folders(folder, type: Literal['sample','patient','mutations']):
    if type == "sample" or type == "patient":
        basefilenames = ["data_clinical_{}.txt".format(type)]
        skiprows=4
        comment='#'
        dtype=str
        INCLUDE_COLUMNS = None
    else:
        basefilenames = ["data_mutations.txt","data_mutations_extended.txt"]
        skiprows=0
        comment='#'
        dtype=str
        INCLUDE_COLUMNS = None
        # INCLUDE_COLUMNS=[
        #     'Hugo_Symbol',
        #     'Chromosome',
        #     'Start_Position',
        #     'End_Position',
        #     'Reference_Allele',
        #     'Tumor_Seq_Allele1',
        #     'Tumor_Seq_Allele2',
        #     'STUDY_ID'
        # ]
        # dtype={
        #     'Chromosome':str,
        #     'Tumor_Seq_Allele1':str,
        #     'Tumor_Seq_Allele2':str,
        #     't_ref_count':'Int64',
        #     't_alt_count':'Int64',
        #     'n_ref_count':'Int64',
        #     'n_alt_count':'Int64',
        #     'Validation_Status':str,
        # }

    files = []
    for basefilename in basefilenames:
        files += glob.glob(os.path.expanduser(folder + "**/" + basefilename))
    print("Found {} {} files".format(len(files), basefilename))
    
    tables = []
    
    for f in tqdm(files):
        parquet_filename = os.path.splitext(f)[0] + '.parquet'

        if os.path.exists(parquet_filename) and os.path.getmtime(parquet_filename) >= os.path.getmtime(f):
            # Use the existing Parquet file
            table = pq.read_table(parquet_filename)
        else:    
            try:
                df = pd.read_csv(f, sep="\t",comment='#',low_memory=False,dtype=dtype)
                df["STUDY_ID"] = f.split('/')[-2]
                if INCLUDE_COLUMNS is not None:
                    df = df[INCLUDE_COLUMNS]
            except:
                print("Error parsing {}: ignoring".format(f))
                continue
            
            # Write the DataFrame to a Parquet file
            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_filename)
        
        tables.append(table)
        
    combined_table = pa.concat_tables(tables, promote=True)
    return combined_table.to_pandas()
