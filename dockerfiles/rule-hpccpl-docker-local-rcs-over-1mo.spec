{
    "files": [{
        "aql": 
        {
            "items.find": 
            {
                "repo": "hpccpl-docker-local",
                "type": "folder",
                "name": {"$match": "*-rc*"},
                "created": {"$before": "1mo"}
            }
        }
    }]
}
