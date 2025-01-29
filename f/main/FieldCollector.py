from f.main.Collector import Collector, CMetaKeys
from typing import Any, Iterable
from f.main.ATPTGrister import ATPTGrister
from dataclasses import dataclass

Entry = dict[str, Any]

@dataclass
class fn: # would have used an enum but it has "name" attr reserved
    """
    default field names that have special handling. to be used as keys for entries in FieldCollector.add_entry()
    values- name, desc, tags, rating, url, repo, author. first 4 are source-specific and prefixed with source_id in the output
    """
    name: str = "name"
    desc: str = "description"
    tags: str = "tags"
    rating: str = "rating"
    # the last 3 are source-agnostic
    url: str = "URL"
    repo: str = "repo"
    author: str = "author"

@dataclass
class FCMetaKeys(CMetaKeys):
    fields: str = "fields"
    """list of possible fields in the source (for making columns before db write)"""

def add_missing(dest: list, source: Iterable):
    dest.extend(i for i in source if i not in dest)

class FieldCollector(Collector):
    def __init__(self, source_id: str, fields: Iterable[str] = [], tags: Iterable[str] | dict[str, Entry] = [], add_repo: bool = False):
        """
        Args:
            source_id (str): a source_id from the data sources table
            fields (list[str], optional): field name must be valid python variable, no spaces. The fields for name, description, tags, rating are processed/aggregated, use the class fn for them them. Fields starting with _ are hidden in the intermediary output. All fields are prefixed with the source_id when assigned.
        """
        super().__init__(source_id, add_repo)
        self.g = ATPTGrister()
        self._prefix = source_id + "_"
        self._fields: list[str] = [self._prefix + i for i in fields]
        """user-defined fields in the output table. passed to self._make_meta_table"""
        self._fn = fn(*(self._prefix + i for i in list(fn().__dict__.values())[:-3]))
        """prefixed default field names"""
        if tags or (fn.tags in fields):
            self._tags_set: set[str] = set()
            """all the tags seen during the main pass. for use with self.make_tag_key"""
            self._tag_key: dict[str, str] = {}
            """stores tag references after generating them from grist"""
            self._og_tag_field = "original tags"  # not a real field name! internal/output purposes only. but idk where else to put it.
            self._fields[self._fields.index(self._fn.tags)] = self._og_tag_field #display original tags first
            self._fields.append(self._fn.tags) # display refs after
        if tags:
            self.make_tag_key(tags)
        
        self.display_fields: list[str] = [i for i in fields if not i.startswith(self._prefix + "_")]
        """
        fields to include in output table (prefixed with source id). can be modified directly or passed into self.output().\n
        includes url as the last element (though it is a key and not a field)
        """
        self.display_fields.append("URL") # URL is the key and not a user-set set field. display it last

        self._entries_dict: dict[str, Entry] = dict()
        """entries stored here"""
   
    #TODO make fields a proper shape wrt type checking
    def add_entry(self, url: str = "", entry: Entry = {}) -> None:
        """
        Args:
            entry (dict[str, Any]): has a few special keys it can take: `["name", "description", "tags", "rating"]` are handled and aggregated in the main table. `"author"` and `"repo"` also reserved and put into separate tables
        """
        url = url or entry[fn.url]
        assert url
        url = self.proccess_url(url)
        tag_field = self._fn.tags

        out: Entry = {}

        for field, value in entry.items():
            match field:
                case fn.tags:
                    if self._tag_key:
                        out[self._og_tag_field] = value
                        out[tag_field] = self._apply_tag_key(value)
                    else:
                        self._tags_set |= set(value)
                        out[self._og_tag_field] = value
                case fn.author:
                    self.add_author(entry[fn.author], url)
                case fn.repo:
                    self.add_repo(entry[fn.repo], url)
                case _:
                    out[self._prefix + field] = value

        if old := self._entries_dict.get(url, None):
            self._p(url, out, old)
            
            if old_tags := old.get(self._og_tag_field, None):
                add_missing(out[self._og_tag_field], old_tags)

            if old_rating := old.get(self._fn.rating, 0):
                    out[self._fn.rating] = max(out.get(fn.rating, 0), old_rating)

        self._entries_dict[url] = out
  
    def make_tag_key(self, tags: Iterable[str] | dict[str, Entry]) -> dict[str, str]:
        """
        Converts literal tags into references for nicer presentation.\n
        Should be called before the main pass if tags are listed upfront, or after if not.

        Args:
            tags (Collection[str] | dict[str, Entry] | None): the set of tags with their fields. Defaults to self.tags

        Returns:
            dict[str, str]: {original_tag: tag_ref}
        """        
        g = self.g

        if isinstance(tags, dict):
            #flatten the tag fields to get the complete set and put it into columns
            tag_col_ids = set("Tag", *(field for fields in tags.values() for field in fields))
            tag_cols = [{"id": col, "fields": {"label": col}} for col in tag_col_ids]
            tags_records = [{"require": {"Tag": tag}, "fields": fields} for tag, fields in tags.items()]
        else:
            tags_records = [{"require": {"Tag": tag}} for tag in tags]
            tag_cols = [{"id": "Tag", "fields": {"label": "Tag"}}]

        if self._fn.tags not in (i["id"] for i in g.list_tables()[1]):
            g.add_tables([{"id": self._fn.tags, "columns": tag_cols}])
        else:
            g.add_update_cols(self._fn.tags, tag_cols)

        g.add_update_records(self._fn.tags, tags_records)
        new_tags = g.list_records(self._fn.tags)[1]
        self._tag_key = {x["Tag"]: x["id"] for x in new_tags}
        return self._tag_key
    
    def _apply_tag_key(self, entry_tags: list[str], ) -> list[str]:
        """turn a single tags list into a grist reference list"""
        return ["L"] + [self._tag_key[tag] for tag in entry_tags]
    
    def _make_meta_table(self) -> dict[str, Any]:
        return {
            FCMetaKeys.source_id: self.source_id,
            FCMetaKeys.fields: self._fields
        }

    def _make_entry_table(self) -> dict[str, list[list[str] | dict[str, Any]]]:
        """
        Returns:
            dict[str, list[list[str] | dict[str, Any]]]: [link to description of output format](https://www.windmill.dev/docs/core_concepts/rich_display_rendering#force-column-order)
        """        
        entries: list[list[str] | Entry] = [self.display_fields]
        entries.extend(v | {fn.url: k} for k, v in self._entries_dict.items())
        return {"table-row-object": entries}
  

    def output(self, display_fields: list[str] = []) -> dict[str, list[Any]]:
        """make a pretty json object for output into windmill.

        Args:
            display_fields (list[str], optional): fields to display in output. values can be "URL" and the keys of self._field_key. Defaults the order fields were given in the constructor, with tag_refs and URL added in last. Fields starting with _ are hidden by default.

        Returns:
            dict[str, list[Any]]: [description of output format](https://www.windmill.dev/docs/core_concepts/rich_display_rendering#render-all)
        """
        self.display_fields = display_fields or self.display_fields

        if not self._tag_key:
            self.make_tag_key(self._tags_set)
            for entry in self._entries_dict.values():
                    if self._og_tag_field in entry:
                        entry[self._fn.tags] = self._apply_tag_key(entry[self._og_tag_field])
        return super().output()