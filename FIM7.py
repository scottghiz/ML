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
print("========== FIM (FIM7.py) ==========")
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
#######################################################

outname = "CSV/final7.csv"
df_0 = pd.read_csv(outname, compression=None, header=0, sep=',')
df_1 = df_0.filter([
                    #'lob',
                    #'callgroup',
                    #'initialintentflag',
                    #'interactiongroup',
                    #'interactiontype',
                    #'sessiontype',
                    #'truechatflag',
                    #'chatflag',
                    #'socialflag',
                    'callflag',
                    #'chatflag7day',
                    #'socialflag7day',
                    'callflag7day',
                    #'chatflagsamequeue',
                    #'socialflagsamequeue',
                    'callflagsamequeue',
                    #'chatflag7daysamequeue',
                    #'socialflag7daysamequeue',
                    'callflag7daysamequeue',
                    'contactflag',
                    'contactflag7day'
                    ], axis=1)

### relabeling data ###

#df_1["truechatflag"].replace({
#  0: "truechatflag-0",
#  1: "truechatflag-1"}, inplace=True)

#df_1["chatflag"].replace({
#  0: "chatflag-0",
#  1: "chatflag-1"}, inplace=True)

#df_1["socialflag"].replace({
#  0: "socialflag-0",
#  1: "socialflag-1"}, inplace=True)

df_1["callflag"].replace({
  0: "callflag-0",
  1: "callflag-1"}, inplace=True)

#df_1["chatflag7day"].replace({
#  0: "chatflag7day-0",
#  1: "chatflag7day-1"}, inplace=True)

#df_1["socialflag7day"].replace({
#  0: "socialflag7day-0",
#  1: "socialflag7day-1"}, inplace=True)

df_1["callflag7day"].replace({
  0: "callflag7day-0",
  1: "callflag7day-1"}, inplace=True)

#df_1["chatflagsamequeue"].replace({
#  0: "chatflagsamequeue-0",
#  1: "chatflagsamequeue-1"}, inplace=True)

#df_1["socialflagsamequeue"].replace({
#  0: "socialflagsamequeue-0",
#  1: "socialflagsamequeue-1"}, inplace=True)

df_1["callflagsamequeue"].replace({
  0: "callflagsamequeue-0",
  1: "callflagsamequeue-1"}, inplace=True)

#df_1["chatflag7daysamequeue"].replace({
#  0: "chatflag7daysamequeue-0",
#  1: "chatflag7daysamequeue-1"}, inplace=True)

#df_1["socialflag7daysamequeue"].replace({
#  0: "socialflag7daysamequeue-0",
#  1: "socialflag7daysamequeue-1"}, inplace=True)

df_1["callflag7daysamequeue"].replace({
  0: "callflag7daysamequeue-0",
  1: "callflag7daysamequeue-1"}, inplace=True)

df_1["contactflag"].replace({
  0: "contactflag-0",
  1: "contactflag-1"}, inplace=True)

df_1["contactflag7day"].replace({
  0: "contactflag7day-0",
  1: "contactflag7day-1"}, inplace=True)

#df_1["callgroup"].replace({
#  "Service": "callgroup-service",
#  "Billing": "callgroup-billing",
#  "Sales": "callgroup-sales",
#  "SIK": "callgroup-sik"}, inplace=True)
#
#df_1["lob"].replace({
#  "Video": "lob-video",
#  "Other": "lob-other",
#  "CDV": "lob-cdv",
#  "XH": "lob-xh",
#  "Mobile": "lob-mobile",
#  "HSD": "lob-hsd"}, inplace=True)
#
#df_1["sessiontype"].replace({
#  "Reactive": "sessiontype-reactive",
#  "Messaging": "sessiontype-messaging",
#  "NPS Survey": "sessiontype-npssurvey",
#  "Multi Session": "sessiontype-multi_session",
#  "Proactive SMS": "sessiontype-proactive_sms",
#  "null": "sessiontype-null",
#  "Chat Handoff": "sessiontype-chathandoff",
#  "Automated Communication": "sessiontype-autocommunication"}, inplace=True)
#
#df_1["interactiongroup"].replace({
#  "Troubleshooting": "interactiongroup-troubleshooting",
#  "Billing": "interactiongroup-billing",
#  "Other": "interactiongroup-other",
#  "Outage": "interactiongroup-outage",
#  "Appointment": "interactiongroup-appointment",
#  "Payments": "interactiongroup-payments",
#  "Sales": "interactiongroup-sales"}, inplace=True)
#
#df_1["interactiontype"].replace({
#  "Channel Issues": "interactiontype-channel_issues",
#  "Video Reboot": "interactiontype-video_reboot",
#  "Connection": "interactiontype-connection",
#  "Change of Service": "interactiontype-chg_of_svc",
#  "Bill Explanation": "interactiontype-bill_explanation",
#  "View Bill": "interactiontype-view_bill",
#  "Identity": "interactiontype-identity",
#  "Video Features": "interactiontype-video_features",
#  "Information": "interactiontype-info",
#  "Apps": "interationtype-apps",
#  "Identified Outage": "interactiontype-indentified_outage",
#  "Credits": "interactiontype-credits",
#  "Remote Issues": "interactiontype-remote_issues",
#  "Repair Cancel": "interactiontype-repair_cancel",
#  "Parental Controls": "interactiontype-parental_controls",
#  "Video Activation": "interactiontype-video-activation",
#  "Make One Time Payment": "interactiontype-make_one_time_payment",
#  "Payment Arrangements": "interactiontype-payment_arrangements",
#  "Account Info": "interactiontype-acct_info",
#  "Accessibility": "interactiontype-accessability",
#  "Netflix": "interactiontype-netflix",
#  "Restore Service": "interactiontype-restore_svc",
#  "Downgrade": "interactiontype-downgrade",
#  "Linked Accounts": "interactiontype-linked_accts",
#  "Device Management": "interactiontype-device_mgmt",
#  "Service": "interactiontype-service",
#  "Troubleshooting": "interactiontype-troubleshooting",
#  "Repair Reschedule": "interactiontype-repair_reschedule",
#  "Dispute": "interactiontype-dispute",
#  "Other": "interactiontype-other",
#  "Refunds": "interactiongroup-refunds",
#  "Amazon": "interactiongroup-amazon",
#  "Device Management": "interactiontype-device_management",
#  "Peacock": "interactiontype-peacock",
#  "Discounts": "interactiontype-discounts",
#  "Disconnect": "interactiontype-disconnect",
#  "Repair Cancel": "interactiontype-repair_cancel"}, inplace=True)


df_1.to_csv('CSV/df_1.csv',index=False)

#### 'join' df_fpx and df_1 #################################
#
#df_result = pd.concat([df_1, df_fpx], axis=1, join="inner")
#df_result.to_csv('CSV/df_result.csv',index=False)  ### DEBUGGING ###
#
##### remove specific columns in dataframe ##########################################
##df_result.drop(['region', 'channel', 'division', 'fullpenultimate', 'platform', 'hashsd', 'hascdv', 'hasvideo', 'hasxh'], axis=1)
#
##### BYPASS FPX #############
##df_result = df_1
#

########################################################################################################################
# Using FPGROWTH #######################################################################################################
########################################################################################################################

#file1 = "CSV/df_1.csv"
#file2 = "CSV/fp_2.csv"
#df_01 = pd.read_csv(file1, compression=None, header=0, sep=',')
#df_02 = pd.read_csv(file2, compression=None, header=0, sep=',')
#df_result = pd.concat([df_01, df_02], axis=1, join="inner")

df_result = df_1
print("=== df_result ===")
print(df_result)


#######################################
#import sys ###########################
#sys.exit() ### EXIT ##################
#######################################

### remove specific columns in dataframe ##########################################
#df_result = df_result.drop(['region', 'channel', 'division', 'fullpenultimate', 'platform', 'hashsd', 'hascdv', 'hasvideo', 'hasxh','endstatus','sessiontype','interactiongroup','interactiontype','initialcontentcode'], axis=1)

print("=== df_result_headers ===")
count = 0
for col in df_result.columns:
    count = count + 1
    print(str(count) + " -- " + col)

permutations = 2**count
print()
print("permutations = " + str(permutations))

df_2 = df_result.applymap(str) ### make all strings in dataframe ###
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

title = "CSV/TEST_FPM7.csv"

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

