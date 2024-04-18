import pandas as pd
import tkinter as tk
from tkinter import filedialog
import os
from datetime import date
from datetime import timedelta

today = date.today()
yesterday = today - timedelta(days = 1)
year = yesterday.strftime("%Y")
month = yesterday.strftime("Y_%m")

#get previous business day. the reconciliation is performed t-1
while yesterday.weekday() >=5:
    yesterday -= timedelta(days=1)
Flash_yesterday = yesterday.strftime("%d.%m.%Y")

#user maintained excel sheet for matrix mapping table of column names between systems. Import this so that we can override the column names from both source systems into a share naming formate
map = pd.read_csv(r'\\sharedrive\map.csv')
dfmap = pd.DataFrame(map)

#open the numbers from trading
try:
    Flash_filepath = fr"\\sharedrive\{year}\Flash"
    Flash_filename = f"FLASH {Flash_yesterday}.csv"
    Flash_file = os.path.join(Flash_filepath, Flash_filename)
    data = pd.read_csv(Flash_file)
    df = pd.DataFrame(data)
except FileNotFoundError:
    print(f"trading havent saved {Flash_filename}. Check with tech")
except Exception as e:
    print("unexpected error when importing flash data", e)

#open the actual numbers. These are saved as unique files per area. Merge each area. 
root = tk.Tk()
root.withdraw()
folder_path = r'\\somelocation'
file_paths = filedialog.askopenfilenames(initialdir=folder_path, title='Select actual files')
ACTUAL = pd.DataFrame()

for file_path in file_paths:
    try:
        data2 = pd.read_csv(file_path)
        ACTUAL = pd.concat([ACTUAL, data2], ignore_index=True, axis=0)
    except FileNotFoundError:
        print(f"the file '{file_path}' hasn't been saved")
    except pd.errors.EmptyDataError:
        print(f"the file '{file_path}' is empty")

root.destroy()

#rename the columns in the two source files based on that mapping file we imported
dictionary = {}
for i in dfmap.index:
    dictionary.update({dfmap['FLASH'][i] : dfmap['EMAIL'][i]})
df.rename(columns=dictionary, inplace=True)

#if any column isn't mapped, we dont' care about it, so drop it
lst = list(dfmap['EMAIL'])
FLASH = df[df.columns.intersection(lst)]


#flash creates duplicate keys vs actual on one column as it breaks out two securities into different types. Make them all stk as that's how they're processed downstream
FLASH.loc[FLASH['Inst Type'] == 'ETF', 'Inst Type'] = 'STK'



#actual as a extra key called source system. This is unique and creates duplicate keys. add accumulation on keys to count, drop any count >1 as it's just a dupe in ACTUAL
ACTUAL['g'] = ACTUAL.groupby(['Desk', 'ID', 'Name', 'book', 'type', 'ccy']).cumcount()
FLASH['g'] = FLASH.groupby(['Desk', 'ID', 'Name', 'book', 'type', 'ccy']).cumcount()

#merge all the trades from one both systems into one file. Outer merge so trades unique to each system are included
compare = pd.merge(FLASH, ACTUAL, on=['Desk', 'ID', 'Name', 'book', 'type', 'ccy', 'g'], how='outer', suffixes=(' Flash', ' ACTUAL'))
compare.drop('g', axis=1)

#create difference columns for each prefex column that has flash/actual suffixes
unique_suffixes = ['FLASH', 'ACTUAL']
result_columns = {}

for suffix in unique_suffixes:
    for col in compare.columns:
        if col.endswith(f' {suffix}'):
            prefix = col.rsplit(f' {suffix}', 1)[0]
            FLASH_col = f'{prefix} FLASH'
            ACTUAL_col = f'{prefix} ACTUAL'

            #check that the prefix for the col has both suffixes
            if FLASH_col in compare.columns and ACTUAL_col in compare.columns:
                result_column = f'{prefix} Diff'
                #check if results col already exists through our loop of each prefix (there's 2 prefixes each)
                if result_column not in result_columns:
                    #doing a subtraction so make na, 0
                    compare[FLASH_col].fillna(0, inplace=True)
                    compare[ACTUAL_col].fillna(0, inplace=True)
                    #subtract to get diff
                    try:
                        compare[result_column] = compare[ACTUAL_col].astype(float) - compare[FLASH_col].astype(float)
                        result_columns[result_column] = None #store result col
                    except(ValueError, TypeError):
                        pass


#reorder the columns so that the new difference column comes after the actual column in the prefix pair (rathat than all of them at the end)
new_column_order = []
for col in compare.columns:
    if col in result_columns:
        prefix = col.rsplit(' ', 1)[0]
        col2 = f'{prefix} ACTUAL'
        column_position = new_column_order.index(col2) + 1
        new_column_order.insert(column_position, col)
    else:
        new_column_order.append(col)

compare = compare[new_column_order]

#now save down as unique files per each unique person 
def filter_and_save(df, column_name, output_folder):
    unique_value = df[column_name].unique()
    #we're saving here come hell or high water!
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for value in unique_value:
        #filter on the key column for uniques
        filtered_df = df[df[column_name] == value]
        
        #only care about trades that have differences > 1,500 and we drop SFX instruments and nulls. SFX aren't in scope, nor test trades that are unmapped
        filtered_df = filtered_df[abs(filtered_df['Reported PnL Diff']) > 1500]
        filtered_df = filtered_df[(filtered_df['Type'] != 'SFX') & (filtered_df['Type'] != '') & (filtered_df['Type'].notna())]
        file_name = f"EVA {value} {yesterday}.csv"
        file_path = os.path.join(output_folder, file_name)

        if not os.path.exists(file_name):
            try:
                filtered_df.to_csv(file_path, index=False)
            except Exception as e:
                print(f'{value} didnt save - {e}')

if __name__ == "__main__":

    column_to_filter = 'Desk'
    output_folder = f'\\someplace\{year}\{month}\EVA'
    filter_and_save(compare, column_to_filter, output_folder)

