import requests
import re
from typing import Any
from f.main.FieldCollector import FieldCollector, fn
import mistune
from pprint import pprint
get_tree = mistune.create_markdown(renderer=None)

gh_regex = r"(https://github\.com/[^/]*/[^/]*)/?$"

c = FieldCollector("Notjuliet_Aweome_Atproto", ["name", "description", "tags"], add_repo=True)

def get_node_text(node) -> str: # thanks chatgpt
    """Recursively extract text from a node and its children."""
    if isinstance(node, str):
        return node
    if "raw" in node:
        return node["raw"]
    if "children" in node:
        return "".join(get_node_text(child) for child in node["children"])
    return ""

def main():

    md : Any = get_tree(
        requests.get("https://raw.githubusercontent.com/notjuliet/awesome-bluesky/refs/heads/main/README.md").text,
    )

    # https://codebeautify.org/python-formatter-beautifier on md is helpful
    current_h2 : list = []
    current_h3 : list = []
    for node in md:
        if node["type"] == "heading" and node["attrs"]["level"] == 2:
            current_h2 = [node["children"][0]["raw"]]
            current_h3 = []
        if node["type"] == "heading" and node["attrs"]["level"] == 3:
            current_h3 = [node["children"][0]["raw"]]
        if node["type"] == "list": #sublist
            list_items = node["children"]
            for item in list_items: # list entries
                # item always has a single child block_text which has two children, link and description
                link = item["children"][0]["children"][0]
                url = link["attrs"]["url"]
                entry = {
                    fn.name: link["children"][0]["raw"],  # raw text of the link
                    fn.desc: "".join(i["raw"] for i in item["children"][0]["children"][1:])[3:],
                    fn.tags: current_h2 + current_h3,
                }
                c.add_entry(url, entry)

    return c.output()