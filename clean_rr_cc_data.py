# -*- coding: utf-8 -*-
"""
@author Matt Harris
"""

import pandas as pd
import time
import numpy as np

def provider_rr(df):
    '''
    Returns the exit date for a DataFrame if
    the exit date is from a RR provider.
    Otherwise it returns 0.
    '''
    if 'rapid' in df['provider'].lower():
        return df['exitdate']
    else:
        return 0


def provider_cc(df):
    '''
    Returns the entry date for a DataFrame if
    the entry date is from a Catholic Charities
    homeless point of entry. Otherwise it returns
    0.
    '''
    if 'catholic charities homeless point of entry' in df['provider'].lower():
        return df['entrydate']
    else:
        return 0


# Read the CSV file into a DataFrame (df)
df = pd.read_csv('rr_cc_data.csv',dtype={'Entry Exit Client Id' : str})

# Rename the columns, dropping unnecessary data/columns
df.columns = ['id','provider','entrydate','exitdate','exitdestination']
df = df.drop('exitdestination', 1)
df = df[df.id > 0]

# Add the columns exit_rr and entry_cc, filling them in
# only for RR or CC providers, respectively
df['exit_rr'] = df.apply(provider_rr,axis=1)
df['entry_cc'] = df.apply(provider_cc,axis=1)
#df['entry_cc'] = pd.to_datetime(df['entry_cc'])
#df['exit_rr'] = pd.to_datetime(df['exit_rr'])

# Remove duplicates for entry date (just take the latest exit)
df = df.sort(['id','entrydate']).groupby(['id','entrydate'],
    as_index=False).last()

# Drop unnecessary columns
df = df.drop(['provider','entrydate','exitdate'],1)

# Now create separate DFs for RR and CC data
rr = df[df['exit_rr'] > 0]
cc = df[df['entry_cc'] > 0]

# Create a cartesian for RR
# This will be used to remove cases where there are multiple
# entries and exits to/from a RR before ever getting to a CC
rr = pd.merge(rr,rr,on='id')

# Merge the RR and CC data and change the data types
merged = pd.merge(rr[['id','exit_rr_x','exit_rr_y']],
	cc[['id','entry_cc']],on='id',how='left')
merged['entry_cc'] = pd.to_datetime(merged['entry_cc'])
merged['exit_rr_x'] = pd.to_datetime(merged['exit_rr_x'])
merged['exit_rr_y'] = pd.to_datetime(merged['exit_rr_y'])

# Create a list of exit_rr_x dates/IDs that should not be used
# because a later RR date should be used instead
donotuse = merged.loc[(merged['exit_rr_x'] < merged['exit_rr_y'])
    & (merged['exit_rr_y'] <= merged['entry_cc'])]

# Add a column to donotuse, setting it equal to 1 for
# every entry in the DF.
donotuse['donotuse'] = 1

# Merge the data with the donotuse DF using a left join.
# This will allow us to see which columns should be used
# and which should be discarded.
merged = pd.merge(merged[['id','exit_rr_x','entry_cc']],
    donotuse[['id','exit_rr_x','donotuse']],
    on=['id','exit_rr_x'],how='left')

# Throw away any rows that should not be used
merged = merged[merged['donotuse'].isnull()]

# Throw away unnecessary columns
merged = merged[['id','exit_rr_x','entry_cc']]

# Save all the exit rr times since we will need all of them
merged_rr = merged[['id','exit_rr_x']]
merged_rr = merged_rr.drop_duplicates()

# Only include rows where the exit_rr is before
# the entry_cc, or where there was no entry into a CC
merged = merged.loc[(merged['exit_rr_x'] <= merged['entry_cc']) |
    (merged['entry_cc'].isnull())]
    
merged = pd.merge(merged_rr,merged,on=['id','exit_rr_x'],how='left')

# Rename columns for export purposes
merged.columns = ['id','exit_rr','entry_cc']

# Take the minimum entry_cc date for each id, exit_rr pair
df = merged.sort(['id','exit_rr']).groupby(['id','exit_rr'], 
    as_index=False).first()

# Calculate days between RR exit and CC entry
df['days_recidivate'] = (df['entry_cc'] - df['exit_rr']) / np.timedelta64(1, 'D')

# Print the DF to a CSV file
df.to_csv('clean_rr_cc.csv',index=False,date_format='%m/%d/%Y',float_format='%.0f')

########################
## Data analysis

# count unique values
#len(pd.unique(df.values.ravel()))