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
        # Add next report date 
        self.next_report_dates = self._api.get_kpi_data_all_instruments(201, 'last', 'latest')

    def create_excel_files(self):
        # looping through all instruments
        for insId, instrument in self._instruments.iterrows():
            stock_prices = self._api.get_instrument_stock_prices(insId)
            reports_quarter, reports_year, reports_r12 = self._api.get_instrument_reports(insId)
            # map the instruments market/country id (integer) to its string representation in the market/country-table
            market = self._markets.loc[instrument['marketId']]['name'].lower().replace(' ', '_')
            country = self._countries.loc[instrument['countryId']]['name'].lower().replace(' ', '_')
            export_path = constants.EXPORT_PATH + f"{country}/{market}/"
            instrument_name = instrument['name'].lower().replace(' ', '_')
            next_report_date = self.next_report_dates.loc[insId].valueStr
            
            # Add meta data sheet
            meta_data = [
            {
            "insId": insId,
            "name": instrument_name,
            "nextReport": pd.to_datetime(next_report_date),
            "urlName": instrument['urlName'],
            "instrument": instrument['instrument'],
            "isin": instrument['isin'],
            "ticker": instrument['ticker'],
            "yahoo": instrument['yahoo'],
            "sectorId": instrument['sectorId'],
            "marketId": instrument['marketId'],
            "branchId": instrument['branchId'],
            "countryId": instrument['countryId'],
            "listingDate": instrument['listingDate'],
            "market": market,
            "country": country,
            "ohlcv_updated": stock_prices.index[0],
            "reports_quarter_updated": reports_quarter.index[0],
            "reports_r12_updated":  reports_r12.index[0],
            "reports_year_updated":  reports_year.index[0],
            }]
            stock_meta = pd.DataFrame(meta_data)

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

        print(f'Set last update {dt.datetime.now().date()} in {constants.EXPORT_PATH + "last_update.txt"}')
        with open(constants.EXPORT_PATH  + 'last_update.txt', "w") as f:
            f.write(f'{dt.datetime.now().date()}')


if __name__ == "__main__":
    excel = ExcelExporter()
    excel.create_excel_files()
