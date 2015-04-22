# -*- coding: utf-8 -*-
"""
@author Matt Harris
"""

import pandas as pd
import time
import numpy as np

def provider_date(df):
    '''
    Returns the exit date for a DataFrame if
    the exit date is from a RR provider.
    Otherwise it returns 0.
    '''
    if 'rapid' in df['provider'].lower():
        return df['exitdate']
    elif 'catholic charities homeless point of entry' in df['provider'].lower():
        return df['entrydate']
    else:
        return 0


def provider_type(df):
    '''
    Returns the exit date for a DataFrame if
    the exit date is from a RR provider.
    Otherwise it returns 0.
    '''
    if 'rapid' in df['provider'].lower():
        return 'RR'
    elif 'catholic charities homeless point of entry' in df['provider'].lower():
        return 'CC'
    else:
        return 'N/A'
        

# Read the CSV file into a DataFrame (df)
#df = pd.read_csv('rr_cc_data.csv',dtype={'Entry Exit Client Id' : str})
df = pd.read_excel('041015_mea_for_abby.xls')

# Rename the columns, dropping unnecessary data/columns
df.columns = ['id','provider','entrydate','exitdate','exitdestination']
df = df.drop('exitdestination', 1)
df = df[df.id > 0]

df['date'] = df.apply(provider_date,axis=1)
df['type'] = df.apply(provider_type,axis=1)

df = df.drop(['provider','entrydate','exitdate'],1)

df = df[df.type != 'N/A']

df['date'] = pd.to_datetime(df['date'])
df = df.sort(['id','date'])

i = 0
rr_dt = None
id_ = None

df2 = pd.DataFrame(columns=['id','exit_rr','entry_cc'])

for index, row in df.iterrows():
    # first RR for this ID
    if row[2] == 'RR'and not id_:
        id_ = row[0]
        rr_dt = row[1]
    # new ID, but did not have a CC for the last RR
    elif row[2] == 'RR' and id_ != row[0] and rr_dt:
        df2.loc[i] = [id_, rr_dt, None]
        id_ = None
        rr_dt = None
        i = i + 1
        id_ = row[0]
        rr_dt = row[1]
    # same ID, but new row for RR
    elif row[2] == 'RR' and id_ == row[0] and rr_dt:
        rr_dt = row[1]
    elif row[2] == 'CC' and id_ == row[0] and rr_dt:
        df2.loc[i] = [id_, rr_dt, row[1]]
        id_ = None
        rr_dt = None
        i = i + 1
    else:
        continue

df = df2;
df['entry_cc'] = pd.to_datetime(df['entry_cc'])
# Calculate days between RR exit and CC entry
df['days_recidivate'] = np.ceil((df['entry_cc'] - df['exit_rr']) 
    / np.timedelta64(1, 'D'))

# Print the DF to a CSV file
df.to_csv('clean_rr_cc.csv',index=False,date_format='%m/%d/%Y',float_format='%.0f')