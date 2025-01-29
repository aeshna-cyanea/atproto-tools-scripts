from requests import get
from bs4 import BeautifulSoup
from typing import Any
from f.main.FieldCollector import FieldCollector, fn

def main() -> dict[str, list[Any]]:

    c = FieldCollector("Skeet_Tools", [fn.name, fn.desc, fn.tags, fn.rating], add_repo=True)

    page_content: Any = BeautifulSoup(
        get("https://dame.blog/skeet-tools/").text, "html.parser"
    )

    sections = page_content.css.select(".post-body > section")
    for section in sections:
        category = section.h2.string
        featured_entry = category.find("Featured") != -1

        for list in section.css.select("ul"):
            current_h3 = list.previous_sibling.previous_sibling
            if current_h3 and current_h3.name == "h3":
                current_h3 = current_h3.string
            else:
                current_h3 = None

            for item in list.css.select("li > a"):
                name = item.string
                parts = name.split(":", 1)  #hope nobody has a colon in their project name
                tool = {
                    fn.name: parts[0].strip(),
                }

                if len(parts) == 2:
                    tool[fn.desc] = parts[1].strip()

                if featured_entry:
                    tool[fn.rating] = 1
                else:
                    tool[fn.tags] = [category]

                if current_h3:
                    tool[fn.tags].append(current_h3)

                item_url = item["href"]
                c.add_entry(item_url, tool)

    return c.output()