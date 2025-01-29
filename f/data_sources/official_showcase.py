import re
import requests
import pyjson5
from f.main.FieldCollector import FieldCollector, Entry, fn

def main() -> dict[str, list]:

    raw_file = requests.get("https://raw.githubusercontent.com/bluesky-social/bsky-docs/refs/heads/main/src/data/users.tsx").text
    
    tags_string = re.search("export const Tags.*= ({.*?^})", raw_file, re.M + re.S).group(1) #type: ignore
    assert isinstance(tags_string, str)
    raw_tags = pyjson5.decode(tags_string)
    del raw_tags["favorite"]
    del raw_tags["opensource"] # we keep track of these separately, don't need them in the key
    tags: dict[str, Entry] = {}
    og_tags_key: dict[str, str] = {} # maps the og lowercase names to their labels
    for og_tag, fields in raw_tags.items():
        tags[fields["label"]] = fields
        og_tags_key[og_tag] = fields.pop("label")

    c = FieldCollector("Official_Showcase", ["name", "description", fn.tags, "rating"], add_repo=True)
    c.make_tag_key(tags)

    raw_entries = re.search(r"User\[\] = (\[\n{.*?\n\])", raw_file, re.S).group(1) # type: ignore
    assert isinstance(raw_entries, str)
    
    # sample entry for reference-
    # title: 'atproto (C++/Qt)',
    # description: 'AT Protocol implementation in C++/Qt',
    # preview: require('./showcase/example-1.png'),
    # website: 'https://github.com/mfnboer/atproto',
    # source: 'https://github.com/mfnboer/atproto',
    # author: 'https://bsky.app/profile/did:plc:qxaugrh7755sxvmxndcvcsgn',
    # tags: ['protocol', 'opensource'],
    
    raw_entries = "".join([x for x in raw_entries.splitlines() if x.find("require(") == -1]) # strip lines with "require("
    entries = pyjson5.decode(raw_entries)
     
    for entry in entries:
        if "website" not in entry: # fix your data guys! https://github.com/bluesky-social/bsky-docs/blob/main/src/data/users.tsx#L846
            entry["website"] = entry["source"] 
        
        url = entry["website"]

        field_key = {
             fn.name: "title",
             fn.desc: "description",
             fn.author: "author",
             fn.repo: "source"
        }
        fields = {k: entry[v] for k,v in field_key.items() if v in entry and entry[v]}

        for og_tag in entry["tags"]:
                if og_tag == "favorite":
                    fields[fn.rating] = 1
                elif og_tag == "opensource":
                    if "source" in entry:
                        if not fields["repo"]:
                             print(f"{url} marked opensource, but no repo field")
                        fields[fn.repo] = entry["source"]
                else:
                    if fn.tags in fields:
                        fields[fn.tags].append(og_tags_key[og_tag])
                    else:
                        fields[fn.tags] = [og_tags_key[og_tag]]
        
        c.add_entry(url, fields)

    return c.output()