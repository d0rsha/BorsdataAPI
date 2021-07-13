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
        self.last_prices = self._api.get_instruments_stock_prices_last()
        self.dates = [self.last_prices.index[0]]
        self.last_prices.reset_index(inplace=True)
        self._api._set_index(self.last_prices, ['date', 'insId'], ascending=False)
        with open(constants.EXPORT_PATH  + 'last_update.txt', "r") as f:
            self.last_update = f.readlines()[0]
        self.new_update = self.dates[0]


    def read_file(self, root, file):
        print("Load " + os.path.join(root, file))

        stock_prices = pd.read_excel(open(os.path.join(root, file), 'rb'), sheet_name='stock_prices', index_col=0)
        reports_quarter = pd.read_excel(open(os.path.join(root, file), 'rb'), sheet_name='reports_quarter')
        reports_quarter.fillna(method='ffill', axis=0, inplace=True)
        reports_quarter['year'] = pd.to_numeric(reports_quarter['year'], downcast='integer')
        self._api._set_index(reports_quarter, ['year', 'period'], ascending=False)

        reports_year = pd.read_excel(open(os.path.join(root, file), 'rb'), sheet_name='reports_year')
        reports_year['year'] = pd.to_numeric(reports_year['year'], downcast='integer')

        reports_r12 = pd.read_excel(open(os.path.join(root, file), 'rb'), sheet_name='reports_r12')
        reports_r12.fillna(method='ffill', axis=0, inplace=True)
        self._api._set_index(reports_r12, ['year', 'period'], ascending=False)
        reports_r12['year'] = pd.to_numeric(reports_r12['year'], downcast='integer')

        stock_meta = pd.read_excel(open(os.path.join(root, file), 'rb'), sheet_name='meta_data', index_col=0)

        return stock_prices, reports_quarter, reports_year, reports_r12, stock_meta

    def get_date_stock_price(self, date, insId):
        if dates.__contains__(date) == False:
            self.last_prices = self.last_prices.append(self._api.get_stock_prices_date(date))
            self.dates.append(date)
        
        return self.last_prices.loc[(date, insId)]
        
    def excel_export(self, stock_prices, reports_quarter, reports_year, reports_r12, stock_meta, ):
        export_path = constants.EXPORT_PATH + f"{stock_meta['country']}/{stock_meta['market']}/"

        # creating path if not exists
        if not os.path.exists(export_path):
            os.makedirs(export_path)

        # creating the writer with export location
        excel_writer = pd.ExcelWriter(export_path + stock_meta['name'] + ".xlsx")
        stock_prices.to_excel(excel_writer, 'stock_prices')
        reports_quarter.to_excel(excel_writer, 'reports_quarter')
        reports_year.to_excel(excel_writer, 'reports_year')
        reports_r12.to_excel(excel_writer, 'reports_r12')
        stock_meta.to_excel(excel_writer, 'meta_data')
        excel_writer.save()
        print(f"Excel exported: {export_path + stock_meta['name'] + '.xlsx'}")

    def update_excel_files(self):
        path = os.getcwd() + "\\" + self._file_path

        # get last updated at 
        old_update_date = self.last_update
        new_update_date = self.new_update

        if (old_update_date == new_update_date): 
            return
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith('.xlsx'):
                    stock_prices, reports_quarter, reports_year, reports_r12, stock_meta = self.read_file(root, file)

                    insId = stock_meta['insId'][0]

                    # Compare, get weekdays to add 
                    daterange = pd.date_range(old_update_date, new_update_date, freq='D').to_series()
                    daterange = daterange[daterange.dt.dayofweek.isin([0, 1, 2, 3, 4])]

                    # Remove first date, we already got it
                    daterange = daterange[1:]

                    for date in daterange:
                        stock_prices[date] = get_date_stock_price(insId, date)

                    stock_meta['ohlcv_updated'] = new_update_date
                    self.excel_export(stock_prices, reports_quarter, reports_year, reports_r12, stock_meta)

        print(f"Set last update {new_update_date} in {constants.EXPORT_PATH + 'last_update.txt'}")
        with open(constants.EXPORT_PATH  + 'last_update.txt', "w") as f:
            f.write(f'{new_update_date}')
                    




if __name__ == "__main__":
    excel = ExcelUpdater()
    excel.update_excel_files()
    
    