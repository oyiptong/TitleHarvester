#!/usr/bin/env python
import json

if __name__ == "__main__":
    with open("interestsData.js", "r") as f:
        ref_data = json.load(f)

    domain_cat_index = {}

    for host, data in ref_data.iteritems():
        domain_cat_index[host] = data["__ANY"]

    with open("textData_host_whitelist.json", "w") as f:
        json.dump(sorted(domain_cat_index.keys()), f, indent=2)

    with open("domain_cat_index.json", "w") as f:
        json.dump(domain_cat_index, f, indent=2)
