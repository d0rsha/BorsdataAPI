from borsdata.borsdata_api import *
import pandas as pd
import os
import datetime as dt
from borsdata import constants as constants



class ExcelUpdater:
    """
    Class for reading .xlsv files and use the BorsdataAPI 
    to append updates with latest prices, reports and KPI's. 
    Save updated df to existing files. 
    """

    def __init__(self):
        self._api = BorsdataAPI(constants.API_KEY)
        self._file_path = constants.EXPORT_PATH

    def update_excel_files(self):
        path = os.getcwd() + "\\" + self._file_path

        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith(".xlsx"):
                    print("Load " + os.path.join(root, file))

                    df = pd.read_excel(open(os.path.join(root, file), 'rb'), sheet_name=constants.SHEET_STOCK_PRICES, index_col=0)
                    stock_prices = pd.read_excel(open(os.path.join(root, file), 'rb'), sheet_name=constants.SHEET_STOCK_PRICES, index_col=0)
                    reports_quarter = pd.read_excel(open(os.path.join(root, file), 'rb'), sheet_name=constants.SHEET_REPORTS_Q)
                    reports_quarter.fillna(method='ffill', axis=0, inplace=True)
                    reports_quarter['year'] = pd.to_numeric(reports_quarter['year'], downcast='integer')
                    self._api._set_index(reports_quarter, ['year', 'period'], ascending=False)

                    reports_year = pd.read_excel(open(os.path.join(root, file), 'rb'), sheet_name=constants.SHEET_REPORTS_Y)
                    reports_year['year'] = pd.to_numeric(reports_year['year'], downcast='integer')

                    reports_r12 = pd.read_excel(open(os.path.join(root, file), 'rb'), sheet_name=constants.SHEET_REPORTS_R12)
                    reports_r12.fillna(method='ffill', axis=0, inplace=True)
                    self._api._set_index(reports_r12, ['year', 'period'], ascending=False)
                    reports_r12['year'] = pd.to_numeric(reports_r12['year'], downcast='integer')

                    stock_meta = pd.read_excel(open(os.path.join(root, file), 'rb'), sheet_name=constants.SHEET_META_DATA, index_col=0)

                    insId = stock_meta['insId'][0]

                    # ToDo 

                    # get last updated at 
                    # get last prices
                    # update




if __name__ == "__main__":
    excel = ExcelUpdater()
    stock_prices, reports_quater, reports_year, reports_r12 = excel.create_excel_files()
    
    