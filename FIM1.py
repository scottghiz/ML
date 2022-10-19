#!/usr/bin/python3

from datetime import datetime

now = datetime.now()

FMT = '%H:%M:%S'
start_time = now.strftime(FMT)
FMT2 = '%Y-%m-%d'
cdate = now.strftime(FMT2)

#print("Start Time   =", start_time)
#print("Current Date =", cdate)

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
print("========== FIM (FIM1.py) ==========")
print()

#######################################################

import configparser

config = configparser.ConfigParser()
config.read('config.ini')

minsupp = config['DEFAULT']['minsupp']
minconf = config['DEFAULT']['minconf']
minlift = config['DEFAULT']['minlift']
slack_on = config['DEFAULT']['slack_on']
tfilter = config['DEFAULT']['target_0']
fullfull = config['DEFAULT']['fullfull']


print(slack_on)
print(tfilter)
print()

minsupp = float(minsupp)
minconf = float(minconf)
minlift = float(minlift)
slack_on = int(slack_on)
tfilter = str(tfilter)

#######################################################

### big original dataframe ############################

outname = "DATA/full.csv"
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


temp_list = []
for x in list_fp_unique:
  temp_list = x.split(";")
  temp_list.append(temp_list)

uniq_temp_list = []
for item in temp_list: 
    if item not in uniq_temp_list: 
        uniq_temp_list.append(item) 

###

full_list = []
for index,row in df_fullpath.iterrows():
  x1 = row['fullpath'].split("; ")
  for x in range(len(x1)):
    x2 = x1[x].split("|")
    temp_list = []
    for y in range(len(x2)):
      temp_list.append(x2[y])
      full_list.append(x2[y])
    temp_list = list(set(temp_list))

uniq_set = set(full_list)
full_list = list(uniq_set)

### create fullpath dataframe with unique columns ######

df_fullpath_0 = pd.DataFrame()
column_number = 0
for y in range(len(full_list)):
  df_fullpath_0.insert(column_number,full_list[y],[],True)
  column_number = column_number + 1

df_fullpath_1 = df_fullpath_0.copy()

column_names = full_list 
df_fullpath_head = pd.DataFrame(columns = column_names)
df_fullpath_all = df_fullpath
fullpath_list = full_list
#print("=== fullpath_list ===")
#print(fullpath_list)

temp_data_list = []
for index,row in df_fullpath_all.iterrows():
  for x in fullpath_list:
    if x in row[0]:
      temp_data_list.append(x) 
    else:
      temp_data_list.append("") 
  df_fullpath_head = df_fullpath_head.append(pd.DataFrame([temp_data_list], columns=fullpath_list), ignore_index=True)
  temp_data_list = []  

#print("=== df_fullpath_head ===")
#print(df_fullpath_head)

### dataframe to csv file containing parsed 'fullpath' ###

df_fullpath_head.to_csv('CSV/fp_test.csv',index=False)  ### TEMP FILE WRITE ###
outname = "CSV/fp_test.csv"                             ### TEMP FILE READ  ### (why?)
df_fpx = pd.read_csv(outname, compression=None, header=0, sep=',')

### replace NaN with columnname-no df_fpx ###

df_xxx = df_fpx
for x in full_list:
  newname = x + "-not"
  df_xxx = df_xxx.rename(columns={x:newname})

#print("=== df_xxx ===")
#print(df_xxx)
df_xxx = df_xxx.fillna(df_xxx.columns.to_series())
#df_xxx.to_csv('CSV/df_xxx.csv',index=False)

df_fpx = df_xxx
df_fpx.to_csv('CSV/df_fpx.csv', compression=None, header=0, sep=',')


#print()
#print("=== df_fpx ===")
#print(df_fpx)
#print()


##########################################################

outname = "DATA/full.csv"
df_0 = pd.read_csv(outname, compression=None, header=0, sep=',')
df_1 = df_0.filter(['region',
                    'division',
                    'channel',
                    'fullpenultimate',
                    'initialintentcode',
                    'initialcontentcode',
                    'sessiontype',
                    'interactiongroup',
                    'interactiontype',
                    'endstatus',
                    'hasvideo',
                    'hashsd',
                    'hascdv',
                    'hasxh',
                    'platform'], axis=1)

### relabeling data ###

df_1["hasvideo"].replace({
  "yes": "hasvideo-yes",
  "no": "hasvideo-no"}, inplace=True)

df_1["hashsd"].replace({
  "yes": "hashsd-yes",
  "no": "hashsd-no"}, inplace=True)

df_1["hascdv"].replace({
  "yes": "hascdv-yes",
  "no": "hascdv-no"}, inplace=True)

df_1["hasxh"].replace({
  "yes": "hasxh-yes",
  "no": "hasxh-no"}, inplace=True)

df_1["sessiontype"].replace({
  "Reactive": "sessiontype-reactive",
  "Messaging": "sessiontype-messaging",
  "NPS Survey": "sessiontype-npssurvey",
  "Multi Session": "sessiontype-multi_session",
  "Proactive SMS": "sessiontype-proactive_sms",
  "Automated Communication": "sessiontype-autocommunication"}, inplace=True)

df_1["interactiongroup"].replace({
  "Troubleshooting": "interactiongroup-troubleshooting",
  "Billing": "interactiongroup-billing",
  "Outage": "interactiongroup-outage",
  "Appointment": "interactiongroup-appointment",
  "Payments": "interactiongroup-payments",
  "Sales": "interactiongroup-sales"}, inplace=True)

df_1["endstatus"].replace({
  "info_delivery": "endstatus-info_delivery",
  "nav_hangup": "endstatus-nav_hangup",
  "self_service": "endstatus-self_service",
  "messaging": "endstatus-messaging"}, inplace=True)

df_1["channel"].replace({
  "Stream": "channel-stream",
  "xFiMobile": "channel-xfimobile",
  "Web": "channel-web",
  "MyAccount": "channel-myaccount",
  "CMP": "channel-cmp"}, inplace=True)

df_1["region"].replace({
  "Freedom": "region-freedom",
  "Western New England": "region-western_new_england",
  "Houston": "region-houston",
  "Greater Boston": "region-greater_boston",
  "California": "region-california",
  "Big South": "region-big_south",
  "Mountain West": "region-mountain_west",
  "Seattle": "region-seattle",
  "Florida": "region-florida",
  "Portland": "region-portland",
  "Beltway": "region-beltway",
  "Chicago": "region-chicago",
  "Heartland": "region-heartland",
  "Twin Cities": "region-twin_cities",
  "Unknown": "region-unknown",
  "Keystone": "region-keystone"}, inplace=True)

df_1["division"].replace({
  "West": "division-west",
  "Central": "division-central",
  "Northeast": "division-northeast",
  "National": "division-national"}, inplace=True)

df_1["platform"].replace({
  "NPS": "platform-nps",
  "MyAccountMobileXAIOS": "platform-myaccountmobilexaios",
  "SMS": "platform-sms",
  "AIQSDK": "platform-aiqsdk",
  "xFiMobileXAAndroid": "platform-xfimobilexaandroid",
  "MyAccountMobileXAAndroid": "platform-myaccountmobilexaandroid",
  "xStreamMobileXAIOS": "platform-xstreammobilexaios",
  "xFiMobileXAIOS": "platform-xfimobilexaios"}, inplace=True)

df_1["interactiontype"].replace({
  "Channel Issues": "interactiontype-channel_issues",
  "Video Reboot": "interactiontype-video_reboot",
  "Connection": "interactiontype-connection",
  "Change of Service": "interactiontype-chg_of_svc",
  "Bill Explanation": "interactiontype-bill_explanation",
  "View Bill": "interactiontype-view_bill",
  "Identity": "interactiontype-identity",
  "Video Features": "interactiontype-video_features",
  "Information": "interactiontype-info",
  "Apps": "interationtype-apps",
  "Identified Outage": "interactiontype-indentified_outage",
  "Credits": "interactiontype-credits",
  "Remote Issues": "interactiontype-remote_issues",
  "Repair Cancel": "interactiontype-repair_cancel",
  "Parental Controls": "interactiontype-parental_controls",
  "Video Activation": "interactiontype-video-activation",
  "Make One Time Payment": "interactiontype-make_one_time_payment",
  "Payment Arrangements": "interactiontype-payment_arrangements",
  "Account Info": "interactiontype-acct_info",
  "Accessibility": "interactiontype-accessability",
  "Netflix": "interactiontype-netflix",
  "Restore Service": "interactiontype-restore_svc",
  "Downgrade": "interactiontype-downgrade",
  "Linked Accounts": "interactiontype-linked_accts",
  "Device Management": "interactiontype-device_mgmt",
  "Service": "interactiontype-service",
  "Troubleshooting": "interactiontype-troubleshooting",
  "Repair Reschedule": "interactiontype-repair_reschedule",
  "Dispute": "interactiontype-dispute",
  "Refunds": "interactiongroup-refunds",
  "Amazon": "interactiongroup-amazon",
  "Device Management": "interactiontype-device_management",
  "Peacock": "interactiontype-peacock",
  "Discounts": "interactiontype-discounts",
  "Disconnect": "interactiontype-disconnect",
  "Repair Cancel": "interactiontype-repair_cancel"}, inplace=True)

df_1['hasvideo'].fillna('hasvideo-unknown', inplace=True)
df_1['hashsd'].fillna('hashsd-unknown', inplace=True)
df_1['hascdv'].fillna('hascdv-unknown', inplace=True)
df_1['hasxh'].fillna('hasxh-unknown', inplace=True)

df_1.to_csv('CSV/df_1.csv',index=False)

### 'join' df_fpx and df_1 #################################

df_result = pd.concat([df_1, df_fpx], axis=1, join="inner")
df_result.to_csv('CSV/df_result.csv',index=False)  ### DEBUGGING ###

#### remove specific columns in dataframe ##########################################
#df_result.drop(['region', 'channel', 'division', 'fullpenultimate', 'platform', 'hashsd', 'hascdv', 'hasvideo', 'hasxh'], axis=1)

#### BYPASS FPX #############
#df_result = df_1


########################################################################################################################
# Using FPGROWTH #######################################################################################################
########################################################################################################################

file1 = "CSV/df_1.csv"
file2 = "CSV/fp_2.csv"
df_01 = pd.read_csv(file1, compression=None, header=0, sep=',')
df_02 = pd.read_csv(file2, compression=None, header=0, sep=',')
df_result = pd.concat([df_01, df_02], axis=1, join="inner")

### remove specific columns in dataframe ##########################################
df_result = df_result.drop(['region', 'channel', 'division', 'fullpenultimate', 'platform', 'hashsd', 'hascdv', 'hasvideo', 'hasxh','endstatus','sessiontype','interactiongroup','interactiontype','initialcontentcode'], axis=1)

print("=== df_result_headers ===")
count = 0
for col in df_result.columns:
    count = count + 1
    print(str(count) + " -- " + col)

permutations = 2**count
print()
print("permutations = " + str(permutations))

#######################################
#import sys ###########################
#sys.exit() ### EXIT ##################
#######################################

df_2 = df_result.applymap(str)
hlist = []
for col in df_2.columns:
  hlist.append(str(col))
#df_2.fillna('', inplace=True)
for x in hlist:
  if "-not" in x:
    newname = x.replace("-not","")
  else:
    newname = x
  df_2 = df_2.rename(columns={x:newname})

#print("=== fim_2.py -- df_2 ===")
#print(df_2)

data0 = df_2.values.tolist()

#get_ipython().system('pip install mlxtend')
#os.system('pip3 install mlxtend')
from mlxtend.preprocessing import TransactionEncoder

te = TransactionEncoder()
te_ary = te.fit(data0).transform(data0)
df_donk = pd.DataFrame(te_ary, columns=te.columns_)
#print("=== fim_2.py -- df_donk ===")
#print(df_donk)

print()
print("=== entering fpgrowth ===")
print()

from mlxtend.frequent_patterns import fpgrowth

# Set 'min_support'... seems to be at least a little above zero
min_support = minsupp 

frequent_itemsets = fpgrowth(df_donk, min_support=minsupp, use_colnames=True)
frequent_itemsets['length'] = frequent_itemsets['itemsets'].apply(lambda x: len(x))
#print(frequent_itemsets)

# Sparse Representation #
oht_ary = te.fit(data0).transform(data0, sparse=True)
sparse_df = pd.DataFrame.sparse.from_spmatrix(oht_ary, columns=te.columns_)
#print(sparse_df)

# Toggle FPGROWTH | NO --> APRIORI #
algo = fpgrowth
verbose_ = 0

df_fpm_sparse = algo(sparse_df, min_support=minsupp, use_colnames=True, verbose=verbose_)
df_fpm_sparse['length'] = df_fpm_sparse['itemsets'].apply(lambda x: len(x))
#print("=== fim_2.py -- df_fpm_sparse ===")
#print(df_fpm_sparse)

# Full Monte #
from mlxtend.frequent_patterns import association_rules as ar

metric_ = "confidence"
min_confidence = minconf
assoc_rules = ar(df_fpm_sparse, metric=metric_, min_threshold=min_confidence)
#print("=== fim_2.py -- assoc_rules ===")
#print(assoc_rules)

assoc_rules = assoc_rules.round(decimals=6)

assoc_rules["antecedent_len"] = assoc_rules["antecedents"].apply(lambda x: len(x))
assoc_rules["consequent_len"] = assoc_rules["consequents"].apply(lambda x: len(x))
sort_by = "conviction"
assoc_rules1 = assoc_rules.sort_values(by=[sort_by],ascending=False)
#print("=== fim_2.py -- assoc_rules1 ===")
#print(assoc_rules1)

sort_on = "confidence"
min_confidence = minconf

sort_on1 = "lift"
min_lift = minlift

condition = (assoc_rules1[sort_on] >= min_confidence) & (assoc_rules1[sort_on1] >= min_lift)
assoc_rules_threshold = assoc_rules1[condition]
assoc_rules_threshold1 = assoc_rules_threshold.sort_values(by=[sort_on])  ### FULL BOAT FIM? ###


if fullfull == 1:
  print()
  print("=== WRITING CSV/FULL_FIM.csv ===")
  print()

  full_fim = "CSV/FULL_FIM.csv"
  assoc_rules_threshold1.to_csv(full_fim, header=["antecedents",
                                "consequents",
                                "antecedent_support",
                                "consequent_support",
                                "support",
                                "confidence",
                                "lift",
                                "leverage",
                                "conviction",
                                "antecedent_len",
                                "consequent_len"], index=False)




#######################################
#import sys ###########################
#sys.exit() ### EXIT ##################
#######################################

### JUST TO TAKE A LOOK AT THINGS DIFFERENTLY #####################################################################
art_temp = assoc_rules_threshold1.sort_values(by=['support'],ascending=False)
art_temp1 = art_temp.loc[art_temp["antecedent_len"] < 6]
art_temp2 = art_temp1.loc[art_temp["consequent_len"] == 1]
art_temp2.to_csv("art_temp2.csv", header=["antecedents",
                              "consequents",
                              "antecedent_support",
                              "consequent_support",
                              "support",
                              "confidence",
                              "lift",
                              "leverage",
                              "conviction",
                              "antecedent_len",
                              "consequent_len"], index=False)


### TEST -- only ONE item in each itemset, antecedent and consequent
assoc_rules_threshold1 = art_temp2

assoc_rules_threshold2 = assoc_rules_threshold1.sort_values(by=['support'],ascending=False)

assoc_rules_threshold2["antecedents"] = assoc_rules_threshold2["antecedents"].apply(lambda x: ', '.join(list(x))).astype("unicode")
assoc_rules_threshold2["consequents"] = assoc_rules_threshold2["consequents"].apply(lambda x: ', '.join(list(x))).astype("unicode")

assoc_rules_threshold2.to_csv("assoc_rules_threshold2.csv", header=["antecedents",
                              "consequents",
                              "antecedent_support",
                              "consequent_support",
                              "support",
                              "confidence",
                              "lift",
                              "leverage",
                              "conviction",
                              "antecedent_len",
                              "consequent_len"], index=False)

assoc_rules_threshold2['consequents'] = assoc_rules_threshold2['consequents'].astype(str)
assoc_rules_threshold2['antecedents'] = assoc_rules_threshold2['antecedents'].astype(str)

#### WRITE CSV FILE ####
### modify csv file even more ######################
df_test_fpm = assoc_rules_threshold2
df_test_fpm = df_test_fpm.astype(str)
df_test_fpm = df_test_fpm[~df_test_fpm.consequents.str.contains("-not")]

### DEBUGGING ########################################################
df_test_fpm.to_csv("CSV/df_test_fpm0.csv", header=["antecedents",
                              "consequents",
                              "antecedent_support",
                              "consequent_support",
                              "support",
                              "confidence",
                              "lift",
                              "leverage",
                              "conviction",
                              "antecedent_len",
                              "consequent_len"], index=False)
######################################################################
####  GOOD TO HERE  ##################################################

df_consequents = df_test_fpm[['consequents']].copy()
df_conseq_unique = df_consequents.consequents.drop_duplicates()
df_conseq_unique.to_csv("CSV/df_conseq_unique.csv",index=False)

import os

title = "CSV/TEST_FPM.csv"

# filtering the rows where Credit-Rating is Fair
#df = df[df['Credit-Rating'].str.contains('Fair')]
#df_test_fpm2 = df_test_fpm[df_test_fpm['consequents'].str.contains(tfilter)]
#df_test_fpm2 = df_test_fpm[df_test_fpm['consequents'].str.contains('yes')]

print("=== WRITING TO CSV FILE ===")

df_test_fpm.to_csv(title, header=["antecedents",
                              "consequents",
                              "antecedent_support",
                              "consequent_support",
                              "support",
                              "confidence",
                              "lift",
                              "leverage",
                              "conviction",
                              "antecedent_len",
                              "consequent_len"], index=False)
  
##############################################################
#### WRITE FPM CSV FILE TO SLACK CHANNEL  ####################
##############################################################
#
#threshold = str(min_confidence)
#threshold1 = str(min_lift)
#
#csv_name = "@" + title
#print(csv_name)
#cmd = "curl -F file="+csv_name+" -F \"initial_comment=FPM - "+sort_on+" threshold : "+threshold+" AND "+sort_on1+" threshold : "+threshold1+" \" -F channels=C02MHBJNLJX -H 'Authorization: Bearer xoxb-2165961063-1525877662391-ycEg7DZtZ4D22Xvr5yfGZPPg' https://slack.com/api/files.upload"
#
#print("=== Slack API ===")
#
#if slack_on == 1:
#        os.system(cmd)

