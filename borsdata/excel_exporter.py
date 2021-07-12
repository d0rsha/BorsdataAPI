from borsdata.borsdata_api import *
import pandas as pd
import os
import datetime as dt
from borsdata import constants as constants


class ExcelExporter:
    """
    A small example class that uses the BorsdataAPI to fetch and concatenate
    instrument data into excel-files.
    """
    def __init__(self):
        self._api = BorsdataAPI(constants.API_KEY)
        self._instruments = self._api.get_instruments()
        self._markets = self._api.get_markets()
        self._countries = self._api.get_countries()

    def create_excel_files(self):
        stocks_meta = []

        # looping through all instruments
        for insId, instrument in self._instruments.iterrows():
            stock_prices = self._api.get_instrument_stock_prices(insId)
            reports_quarter, reports_year, reports_r12 = self._api.get_instrument_reports(insId)
            # map the instruments market/country id (integer) to its string representation in the market/country-table
            market = self._markets.loc[instrument['marketId']]['name'].lower().replace(' ', '_')
            country = self._countries.loc[instrument['countryId']]['name'].lower().replace(' ', '_')
            export_path = constants.EXPORT_PATH + f"{country}/{market}/"
            instrument_name = instrument['name'].lower().replace(' ', '_')
            
            # Add meta data sheet
            updated_at = [
            {
            "insId": insId,
            "ticker": instrument['ticker'],
            "name": instrument_name,
            "ohlcv_updated": stock_prices.index[0],
            "reports_quarter_updated": reports_quarter.index[0],
            "reports_r12_updated":  reports_r12.index[0],
            "reports_year_updated":  reports_year.index[0],
            }]
            stock_meta = pd.DataFrame(updated_at)
            stocks_meta.append(stock_meta)

            # creating necessary folders if they do not exist
            if not os.path.exists(export_path):
                os.makedirs(export_path)
            # creating the writer with export location
            excel_writer = pd.ExcelWriter(export_path + instrument_name + ".xlsx")
            stock_prices.to_excel(excel_writer, 'stock_prices')
            reports_quarter.to_excel(excel_writer, 'reports_quarter')
            reports_year.to_excel(excel_writer, 'reports_year')
            reports_r12.to_excel(excel_writer, 'reports_r12')
            stock_meta.to_excel(excel_writer, 'meta_data')
            excel_writer.save()
            print(f'Excel exported: {export_path + instrument_name + ".xlsx"}')

        meta = pd.concat(stocks_meta, axis=0, ignore_index=True)
        # creating the writer with export location
        excel_writer = pd.ExcelWriter(constants.EXPORT_PATH + "last_update.xlsx")
        meta.to_excel(excel_writer, 'Updated timestamps')
        excel_writer.save()
        print(f'Excel exported: {constants.EXPORT_PATH + "updated.xlsx"}')

        print(f'Set last update {dt.datetime.now().date()} in {constants.EXPORT_PATH + "last_update.txt"}')
        with open(constants.EXPORT_PATH  + 'last_update.txt', "w") as f:
            f.write(f'{dt.datetime.now().date()}')


if __name__ == "__main__":
    excel = ExcelExporter()
    excel.create_excel_files()
