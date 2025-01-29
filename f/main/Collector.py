import re
from typing import Any
from collections import defaultdict
from pprint import pformat
from dataclasses import dataclass

@dataclass(kw_only=True)
class CMetaKeys:
    source_id: str = "source id"


# TODO add other forges besides github
gh_regex = r"(https://github\.com/[^/]*/[^/]*)/?$"
def check_repo(repo: str) -> str | None:
    """check if a repo link is to a known forge (only github atm)"""
    repo_match = re.search(gh_regex, repo) 
    return repo_match.group(1) if repo_match else None

class Collector:
    def __init__(self, source_id: str, check_repos: bool = False) -> None:
        """
        functions to collect and proccess links into a standardized format

        Args:
            source_id (str) []): source_id from data sources table
            add_repo (bool, optional): whether to check for repo link when adding entries. Defaults to False.
        """        
        self._source_id = source_id
        self._entries: set[str]  = set()
        self._repos: defaultdict[str, set[str]] = defaultdict(set) # one repo can host the code for many sites
        """dict of the form {repo_url: {sites}}"""
        self._authors: defaultdict[str, set[str]] = defaultdict(set) # one author can contribute to many sites
        """dict of the form {did: {sites}}"""
        self._check_repos = check_repos

    @property
    def source_id(self) -> str:
        return self._source_id

    def add_repo(self, repo:str, url:str):
        self._repos[repo.rstrip("/ ")].add(url.rstrip("/ "))

    def add_author(self, author:str, url:str):
        self._authors[author.rstrip("/ ")].add(url.rstrip("/ "))
    
    def proccess_url(self, url: str):
        url = url.rstrip("/ ")
        if self._check_repos:
            repo = check_repo(url)
            if repo:
                self.add_repo(repo, repo)
        return url
    
    #TODO add better logging of duplicate entries within a single source 
    def _p(self, key: str, new: Any = None, old: Any = None) -> None:
        if old and new:
            print(f"\nDuplicate for {key}:\n{pformat(old)}\n->\n{pformat(new)}")
        else:
            print(f"\nDuplicate for {key}")

    def add_entry(self, url: str):
        url = self.proccess_url(url)
        if url in self._entries:
            self._p(url)
            return
        self._entries.add(url)

    def _make_entry_table(self) -> dict[str, Any]: #TODO add in other display modes as types
        return {"table-row-object": [{"URL": entry} for entry in self._entries]}
    
    def _make_meta_table(self):
        return {
            CMetaKeys.source_id: self.source_id,
        }
    
    def output(self):
        return { "render_all": [
            self._make_meta_table(),
            self._make_entry_table(),
            {"table-row-object": [{"URL": k, "Sites": list(v)} for k, v in self._repos.items()]},
            {"table-row-object": [{"User": k, "Sites": list(v)} for k, v in self._authors.items()]}
        ]
        }