import typing as t

import gspread

from constants import SERVICE_ACCOUNT


class GSheet:
    def __init__(self):
        self.spreadsheet_name = "Copy MarTech Manager \
- Ads & Acquisition HW - dataset"

    def _open_spreadsheet(self):
        google_creds = gspread.service_account_from_dict(SERVICE_ACCOUNT)
        return google_creds.open(self.spreadsheet_name)

    def _open_worksheet(self, worksheet_name: str):
        return self._open_spreadsheet().worksheet(worksheet_name)

    def get_all_data(
        self,
        worksheet_name: str
    ) -> list[dict[str, t.Any]]:
        '''Function to get all info from selected worksheet'''
        worksheet = self._open_worksheet(worksheet_name)
        return worksheet.get_all_records()

    def update_row(
        self,
        worksheet_name: str,
        cells_scope: str,
        values: list[list[str]]
    ) -> list[list[str]]:
        '''Function to insert/update info to selected area. Example:'A7:E9'''
        return self._open_worksheet(worksheet_name).update(cells_scope, values)
