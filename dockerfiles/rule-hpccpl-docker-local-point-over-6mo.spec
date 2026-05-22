{
    "files": [{
        "aql": 
        {
            "items.find": 
            {
                "repo": "hpccpl-docker-local",
                "path": {"$match": "platform-core-ln*"},
                "created": {"$before": "6mo"},
                "type": "folder",
                "name": {"$nmatch": "*-latest"},
                "name": {"$match": "*${key1}.*"},
                "name": {"$nmatch": "*-rc*"}
            }
        }
    }]
}
