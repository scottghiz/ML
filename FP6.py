#!/usr/bin/python3

from datetime import datetime

now = datetime.now()

FMT = '%H:%M:%S'
start_time = now.strftime(FMT)
FMT2 = '%Y-%m-%d'
cdate = now.strftime(FMT2)

print("Start Time   =", start_time)
print("Current Date =", cdate)

######################################################

# Define IAM role
import boto3
import re
import s3fs
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import sagemaker
import time
from sagemaker import get_execution_role
from sagemaker.inputs import TrainingInput
from sagemaker.serializers import CSVSerializer

#######################################################
print()
print("========== FP6.py ==========")
print()
#######################################################

import configparser

config = configparser.ConfigParser()
config.read('config.ini')

minsupp = config['DEFAULT']['minsupp']
minconf = config['DEFAULT']['minconf']
minlift = config['DEFAULT']['minlift']

minsupp = float(minsupp)
minconf = float(minconf)
minlift = float(minlift)

#######################################################

### big original dataframe ############################

outname = "CSV/full.csv"
df_0 = pd.read_csv(outname, compression=None, header=0, sep=',')

### break up the fullpath into workable data segments ###

df_fullpath = df_0.filter(['fullpath'], axis=1)

### let's do some feature engineering ###

#list_fp_unique = df_fullpath['fullpath'].unique()
df_fp_unique = df_fullpath['fullpath'].unique()

list_fp_unique = []
for index, row in df_fullpath.iterrows():
  temp_list = []
  temp_list = (row['fullpath'])
  temp_list2 = temp_list.split(";")
  for x in temp_list2:
    x = str(x)
    y = x.replace(" ","")

    if y not in list_fp_unique:
      list_fp_unique.append(y)

file1 = open("DATA/fp_1.txt", "a")  # append mode -- fp_2.txt output
for x in list_fp_unique:
  file1.write(x + "\n")
file1.close()


