import sys

import numpy as np
import pandas as pd

def get_and_save_vix_contago(destination_folder):
    """
    Get and download data from VIX central and save as CSV.
    :param destination_file: data destination folder.
    :return: True if success, false otherwise.
    """
    print('Downloading data from VIX central')
    
    try:
        df = pd.read_html('http://vixcentral.com/historical/?days=100000')[0]
        df.columns = df.iloc[0]
        df.drop(index=0, inplace=True)
        df.drop(index=len(df), inplace=True)
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.set_index('Date').sort_index()
        df['Contango 2/1'] = df['Contango 2/1'].str.replace('%', '').astype(float).mul(0.01)
        df['Contango 7/4'] = df['Contango 7/4'].str.replace('%', '').astype(float).mul(0.01)
        df['Con 7/4 div 3'] = df['Con 7/4 div 3'].str.replace('%', '').astype(float).mul(0.01)
        for c in df:
            df[c] = df[c].replace('-', np.nan)
            df[c] = df[c].astype(float)
        print(f'Saving data to {destination_folder}/vix_contago.csv')
        df.to_csv(f'{destination_folder}/vix_contago.csv')
        print(f'Data has successfully been saved')
        return True
    except Exception as e:
        print(f'get_and_save_vix_contago(): Fail to get data from VIX central. {e}')
        return False

if __name__ == '__main__':
    if get_and_save_vix_contago(sys.argv[1]):
        sys.exit(0)
    else:
        sys.exit(1)