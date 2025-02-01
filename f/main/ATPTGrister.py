from typing import Any, Iterable
from pygrister.api import GristApi
import wmill
from json import loads

class CustomGrister(GristApi):
    """raises IOError with the request response on http error"""
    def apicall(self, url: str, method: str = 'GET', headers: dict | None = None, params: dict | None = None, json: dict | None = None, filename: str = '') -> tuple[int, Any]:
        resp =  super().apicall(url, method, headers, params, json, filename)
        if self.resp_code != 200:
            raise IOError(
                self.resp_code, f"{self.resp_reason}: {self.resp_content}"
            )
        return resp

    # don't like doing this but the put columns enpdoint has really weird behaviour, sometimes it invents a new id for you and makes a new column with it
    # also changed the noadd and noupdate default params, kinda weird to have both of those true by default.
    def add_update_cols(self, table_id: str, cols: list[dict], noadd: bool = False, noupdate: bool = False, replaceall: bool = False, doc_id: str = '', team_id: str = ''):
        if replaceall:
            return super().add_update_cols(
                table_id, cols, noadd, noupdate, replaceall, doc_id, team_id
            )
        
        target_col_ids = {col["id"] for col in cols}
        old_cols = self.list_cols(table_id)[1]
        col_ids: set[str] = {x["id"] for x in old_cols}
        if (new_col_ids := target_col_ids - col_ids) and not noadd:
            col_ids |= set(self.add_cols(table_id, [col for col in cols if col["id"] in new_col_ids])[1])
        elif (update_col_ids := col_ids & target_col_ids) and not noupdate:
            super().add_update_cols(table_id, [col for col in cols if col["id"] in update_col_ids], noadd=True)
        return int(self.resp_code), col_ids

    def get_colRef(self, table_id: str, col_id: str) -> int | None:
        return next((col["fields"]["colRef"] for col in self.list_cols(table_id)[1] if col["id"] == col_id), None)

    def get_colRefs(self, table_id: str, col_ids: Iterable[str], format: bool = True):
        """
        finds colRefs for a set of columns, returned formatted as a json array string

        Args:
            table_id (str): the grist table id
            col_ids (set[str]): a list of column ids
            format (bool, optional): whether to return a list instead of a key. Defaults to True.

        Returns:
            str | dict[str,int]: either the {id:colref, ...} dict, or a string with a list of colRefs
        """
        ref_key: dict[str, int] = {col["id"]: col["fields"]["colRef"] for col in self.list_cols(table_id)[1] if col["id"] in col_ids}
        if format:
            return str(list(ref_key.values()))
        return ref_key


def ATPTGrister() -> CustomGrister:
    """Get configured GristApi client"""
    grist_config = loads(wmill.get_variable("f/main/grist_config"))
    grist_config["GRIST_API_KEY"] = wmill.get_variable("u/autumn/GRIST_API_KEY")
    return CustomGrister(grist_config)
