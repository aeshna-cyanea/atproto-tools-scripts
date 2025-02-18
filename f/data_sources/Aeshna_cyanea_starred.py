import requests
import wmill
from f.main.Collector import Collector, ef, normalize
from f.main.ATPTGrister import make_timestamp

#TODO add updatedAt check
query = """
query getList($after: String) {
  node(id: "UL_kwDOCyNr884ATJdC") {
    ... on UserList {
      updatedAT
      items(first: 100, after: $after) {
        totalCount
        nodes {
          ... on RepositoryInfo {
            url
            homepageUrl
            description
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  }
  rateLimit {
    cost
    remaining
    limit
    resetAt
  }
}
"""


def main():
    c = Collector("Aeshna_cyanea_starred", fields=[ef.DESC], add_repos=True)

    headers = {
        "Authorization": f"Bearer {wmill.get_variable('u/autumn/github_key')}",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    after_cursor = None
    while True:
        r = requests.post(
            "https://api.github.com/graphql", json={"query": query, "variables": {"after": after_cursor}}, headers=headers
        ).json()
        node = r["data"]["node"]
        if (current_timestamp := make_timestamp(node["updatedAt"])) == c.last_update_timestamp:
            return
        else:
            c.current_update_timestamp = current_timestamp
        items = node["items"]
        if items["pageInfo"]["hasNextPage"]:
            after_cursor = items["pageInfo"]["endCursor"]
        else:
            after_cursor = None
        
        for node in items["nodes"]:
            repo_url = node["url"]
            homepage = node["homepageUrl"]
            entry = {
                ef.URL: homepage or repo_url,
            }
            if desc := node["description"]:
                entry[ef.DESC] = desc
            c.add_site(entry)
            if homepage:
                c.add_repo_site(normalize(homepage), repo_url)
        
        if not after_cursor:
            break
    return c.output()

if __name__ == "__main__":
    main()
