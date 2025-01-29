import re
import requests
from f.main.ATPTGrister import ATPTGrister
from typing import Any, cast
from FieldCollector import FCMetaKeys as mk, fn

def sql_select_fields_from(items: list[str], fields: list[str], table: str):
    return f"SELECT {", ".join(fields)} FROM {table} WHERE name IN ('{"', '".join(items) }')" 

table_names = ["Sites", "Repos", "Authors"]
table_names = [i + "_test" for i in table_names] # for testing
entries_table, repos_table, authors_table = table_names
g = ATPTGrister()

authors_key: dict[str, str] = {} #TODO populate with handles from Authors table
did_regex = r"(did:[a-z0-9]+:(?:(?:[a-zA-Z0-9._-]|%[a-fA-F0-9]{2})*:)*(?:[a-zA-Z0-9._-]|%[a-fA-F0-9]{2})+)(?:[^a-zA-Z0-9._-]|$)"
def lookup_did(author: str):
    author = author.rstrip("/ ")
    if author in authors_key:
        return authors_key[author]
    
    did_match = re.search(did_regex, author)
    if did_match:
        authors_key[author] = did_match.group(1)
    elif author.startswith("https://bsky.app/profile/"):
        r = requests.get(f"https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle?handle={author[25:]}")
        if r.ok:
            authors_key[author] = r.json()["did"]
        else:   
            print(f"could not resolve bsky handle {author}! {r.reason} {r.content!r}")
    else:
        raise KeyError(f"could not resolve {author} as a did!")
    #TODO support other apps such as ouranos, maybe borrow code from pdsls redirector
    return authors_key.get(author, None)

def put_get_key(table: str, entries: list[dict[str, Any]] | list[str], keyfield: str = fn.url, strip = True) -> dict[str, dict[str, Any]]:
    """
    sends entries to grist and gets thir records

    Args:
        table (str): the table id
        records (list[dict[str, Any]] | list[str]): entries from the previous step
        keyfield (str, optional): field to require (primary key). Defaults to "URL".
        strip (bool, optional): if true, leave out other fields. Defaults to True

    Returns:
        dict[str, dict[str, Any]]: the resulting records, indexed by keyfield
    """
    assert len(entries) > 0
    if isinstance(entries[0], str):
        entries = cast(list[str], entries)
        entries_set: set[str] = set(entries)
        out = [{"require": {keyfield: x}} for x in entries]
    else:
        entries = cast(list[dict[str, Any]], entries)
        entries_set = {x[keyfield] for x in entries}
        if strip:
            out = [{"require": {keyfield: x[keyfield]}} for x in entries]
        else:
            out = [{"require": {keyfield: x.pop(keyfield)}, "fields": x} for x in entries]
    g.add_update_records(table, out)
    #TODO convert this list_records to a sql query based on presence of the relevant timestamp instead of a full list. the non-sql filters can only test for value, not existence
    new_records : list[dict[str, Any]] = g.list_records(table)[1]
    return {x[keyfield]: x for x in new_records if x[keyfield] in entries_set}

#TODO automate more metadata (column type, width etc)
#TODO add some way to track removals of sites from sources.
def make_table_cols(source : str, target_fields: set[str] = set()):
    """
    makes columns in the main table for the current data source

    Args:
        source (str): the data source id
        target_fields (list[str], optional): fields to make into columns. always makes a timestamp column.
    
    Returns:
        list[str]: the column ids
    """
    timestamp_id = source + "_updatedAt"
    target_fields = target_fields.copy() | {timestamp_id}
    
    cols = []
    timestamp_fields = {
                    "type": "DateTime:America/Los_Angeles",
                    "recalcWhen": 0,
                    "widgetOptions": {
                        "widget": "TextBox",
                        "dateFormat": "YYYY-MM-DD",
                        "timeFormat": "HH:mm z",
                        "isCustomDateFormat": False,
                        "isCustomTimeFormat": False,
                        "alignment": "left"
                    },
                    "isFormula": False,
                    "formula": "NOW()"
    }
    for col_id in target_fields:
        entry : dict[str, Any] = {
            "id": col_id,
            "fields": {
                "label": col_id.replace("_", " ")
            }
        }
        # TODO add more formatting rules. (column type, width etc)
        fields = entry["fields"]
        match col_id.split("_")[-1]:
            case "tags":
                fields |= {
                    "type": f"RefList:{col_id}",
                    "visibleCol": g.get_colRef(col_id, "Tag"),
                }
            case "rating":
                fields |= {
                    "type": "Numeric",
                }
            case "updatedAt": # need to add it here to get its own id later 
                fields |= timestamp_fields
        cols.append(entry)

    g.add_update_cols(entries_table, cols)[1]
    new_refs = g.get_colRefs(entries_table, set(target_fields))
    timestamp_fields["recalcDeps"] = new_refs
    g.update_cols(entries_table, [{"id": timestamp_id, "fields": timestamp_fields}])

def main(data: Any):
    meta: dict[str, Any] = data['render_all'][0]
    source : str = meta[mk.source_id]

    entries : list[dict[str, Any]] = data['render_all'][1]['table-row-object']
    columns: set[str] = set()
    to_pop: list[str]= [] # fields not needed in the db

    if mk.fields in meta:
        fields: list[str] = meta[mk.fields] #TODO get a better lib to type-check parsed json
        for field in fields:
            if field.startswith(source + "_"):
                columns.add(field)
            elif not field == fn.url:
                to_pop.append(field)
        entries = entries[1:]
        if to_pop:
            for entry in entries:
                for field in to_pop: #gross. maybe we should change the output layout to table-col instead of table-row-object
                    entry.pop(field, None)
    
    make_table_cols(source, columns)
    #TODO add logic that checks (when PUTting non-forge links to sites)- if a link is the same as its forge, replace it with the non-forge version (don't add a new record to sites)
    single_table_key = put_get_key(entries_table, entries, strip=False)

    repos: list[dict[str, str | list[str]]] = data['render_all'][2]['table-row-object']
    authors: list[dict[str, str |list[str]]] = data['render_all'][3]['table-row-object']

    if repos:
        repos_records = [{
            "require": {"URL": entry["URL"]},
            "fields": {"Sites_refs": ["L"] + [single_table_key[url]["id"] for url in entry["Sites"]]}
        } for entry in repos]
        g.add_update_records(repos_table, repos_records)
            
    if authors:
        def make_author_record(entry):
            author = entry["User"]
            if did := lookup_did(author):
                require = {"DID": did} 
            else:
                require = {"Generic_Website": author}

            sites_refs = ["L"] + [single_table_key[url]["id"] for url in entry["Sites"]]
            return {"require": require, "fields": {"Sites_refs": sites_refs}}

        authors_records = [make_author_record(i) for i in authors]
        g.add_update_records(authors_table, authors_records)
        