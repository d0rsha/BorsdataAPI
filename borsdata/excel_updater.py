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
        self.last_update = pd.to_datetime(self.last_update)
        self.new_update = self.dates[0]
        self.next_report_dates = pd.DataFrame()


    def read_file(self, root, file):
        # print("Load " + os.path.join(root, file))
        stock_prices = pd.read_excel(open(os.path.join(root, file), 'rb'), sheet_name='stock_prices', index_col=0)
        reports_quarter = pd.read_excel(open(os.path.join(root, file), 'rb'), sheet_name='reports_quarter')
        if reports_quarter.empty:
            print(f'Warning: File {os.path.join(root, file)} is missing reports_quarter')
        else:
            reports_quarter.fillna(method='ffill', axis=0, inplace=True)
            reports_quarter['year'] = pd.to_numeric(reports_quarter['year'], downcast='integer')
            self._api._set_index(reports_quarter, ['year', 'period'], ascending=False)

        reports_year = pd.read_excel(open(os.path.join(root, file), 'rb'), sheet_name='reports_year')
        if reports_year.empty:
            print(f'Warning: File {os.path.join(root, file)} is missing reports_year')
        else:
            reports_year['year'] = pd.to_numeric(reports_year['year'], downcast='integer')

        reports_r12 = pd.read_excel(open(os.path.join(root, file), 'rb'), sheet_name='reports_r12')
        if reports_r12.empty:
            print(f'Warning: File {os.path.join(root, file)} is missing reports_r12')
        else:
            reports_r12.fillna(method='ffill', axis=0, inplace=True)
            reports_r12['year'] = pd.to_numeric(reports_r12['year'], downcast='integer')
            self._api._set_index(reports_r12, ['year', 'period'], ascending=False)

        stock_meta = pd.read_excel(open(os.path.join(root, file), 'rb'), sheet_name='meta_data', index_col=0)
        return stock_prices, reports_quarter, reports_year, reports_r12, stock_meta


    def get_date_stock_price(self, date, insId):
        if self.dates.__contains__(date) == False:
            prices = self._api.get_stock_prices_date(date.strftime('%Y-%m-%d'))
            prices.reset_index(inplace=True)
            self._api._set_index(prices, ['date', 'insId'], ascending=False)
            self.last_prices = self.last_prices.append(prices)
            self.dates.append(date)

        return self.last_prices.loc[(date, insId)]
        

    def excel_export(self, stock_prices, reports_quarter, reports_year, reports_r12, stock_meta, ):
        export_path = constants.EXPORT_PATH + f"{stock_meta['country'].loc[0]}/{stock_meta['market'].loc[0]}/"

        if not os.path.exists(export_path):
            os.makedirs(export_path)

        # Bug: Cant call to_excel(..) with df containing columns but 0 rows https://github.com/pandas-dev/pandas/issues/19543
        excel_writer = pd.ExcelWriter(export_path + stock_meta['name'].loc[0] + ".xlsx")
        stock_prices.to_excel(excel_writer, 'stock_prices')
        reports_quarter.to_excel(excel_writer, 'reports_quarter')
        reports_year.to_excel(excel_writer, 'reports_year')
        reports_r12.to_excel(excel_writer, 'reports_r12')
        stock_meta.to_excel(excel_writer, 'meta_data')
        excel_writer.save()
        print(f"Excel updated: {export_path + stock_meta['name'].loc[0] + '.xlsx'}")


    def update_excel_files(self):
        path = os.getcwd() + "\\" + self._file_path

        if (self.last_update >= self.new_update):
            print(f"No need to update, latest update date: {self.new_update}")
            return
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith('.xlsx'):
                    stock_prices, reports_quarter, reports_year, reports_r12, stock_meta = self.read_file(root, file)

                    insId = stock_meta['insId'][0]
                    wasUpdated = False

                    ##############################
                    # Check and Update last price
                    start_date = self.last_update if (self.last_update > stock_prices.index[0]) else stock_prices.index[0]
                    daterange = pd.date_range(start_date, self.new_update, freq='D').to_series()
                    daterange = daterange[daterange.dt.dayofweek.isin([0, 1, 2, 3, 4])]

                    # Remove first date, we already got it
                    daterange = daterange[1:]

                    for date in daterange:
                        stock_prices.loc[date] = self.get_date_stock_price(date, insId)
                        wasUpdated = True

                    stock_prices.sort_index(inplace=True, ascending=False)
                    stock_meta['ohlcv_updated'] = self.new_update

                    ##############################
                    # Check and Update last report 
                    next_report = pd.to_datetime(stock_meta['nextReport'][0])
                    next_report_plus_delta = next_report + pd.Timedelta("10 days")
                    update = (self.new_update > next_report_plus_delta) if next_report else False

                    if update:
                        # We just need the very last updated report
                        reports_quarter_updated, reports_year_updated, reports_r12_updated = self._api.get_instrument_reports(insId, 1, 1)

                        if self.next_report_dates.empty:
                            self.next_report_dates = self._api.get_kpi_data_all_instruments(201, 'last', 'latest')

                        if not reports_quarter_updated.empty and not reports_quarter_updated.index[0] in reports_quarter.index:
                            reports_quarter = pd.concat([reports_quarter_updated, reports_quarter])
                        if not reports_year_updated.empty and not reports_year_updated.index[0] in reports_year.index:
                            reports_year = pd.concat([reports_year_updated, reports_year])
                        if not reports_r12_updated.empty and not reports_r12_updated.index[0] in reports_r12.index:
                            reports_r12 = pd.concat([reports_r12_updated, reports_r12])

                        next_report_date = self.next_report_dates.loc[insId].valueStr
                        stock_meta['nextReport'] = pd.to_datetime(next_report_date)
                        wasUpdated = True

                    if wasUpdated:
                        self.excel_export(stock_prices, reports_quarter, reports_year, reports_r12, stock_meta)

        print(f"Set last update {self.new_update} in {constants.EXPORT_PATH + 'last_update.txt'}")
        with open(constants.EXPORT_PATH  + 'last_update.txt', "w") as f:
            f.write(f'{self.new_update}')



if __name__ == "__main__":
    excel = ExcelUpdater()
    excel.update_excel_files()
    
    