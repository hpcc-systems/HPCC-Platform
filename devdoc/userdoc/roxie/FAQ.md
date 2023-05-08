
# ROXIE FAQs

1. **How I can compile a query on a containerized or cloud-based system?**

**Answer**:
```
Same way as bare metal. Command line, or with the IDE, or from ECL Watch. Just point to the HPCC Systems instance to compile.
For Example:
ecl deploy <target> <file>
```


2. **How do I copy queries from an on-prem cluster to Azure?**

**Answer**:
```
The copy query command – use the Azure host name or IP address for the target.
For example:
ecl queries copy <source_query_path> <target_queryset>
```

3. **How can I get the IP address for the Azure target cluster?**

**Answer**:
```
Use the "kubectl get svc" command. Use the external IP address listed for ECL Watch.
kubectl get svc
```

4. **Do we have to have use the DNSName or do we need to use the IP address?**

**Answer**:
```
If you can reach ECL Watch with the DNS Name then it should also work for the command line.
```

5. **How can I find the ECL Watch or Dali hostname?**

**Answer**:
```
If you did not set up the containerized instance, then you need to ask your Systems Administrator or whomever set it up..
```

6. **How do I publish a package file?**

**Answer**:
```
Same way as bare metal.
To add a new package file: ecl packagemap add or
To copy exisitng package file : ecl packagemap copy
```

7. **How do I check the logs?**

**Answer**:
```
kubectl log <podname>
in addition you can use -f (follow) option to tail the logs. Optionally you can also issue the <namespace> parameter.
For example:
kbectl log roxie-agent-1-3b12a587b –namespace MyNameSpace
Optionally, you may have implemented a log-processing solution such as the Elastic Stack (elastic4hpcclogs).
```

8. **How do I get the data on to Azure?**

**Answer**:
```
Use the copy query command and copy or add the Packagemap.
With data copy start in the logs…copy from remote location specified if data doesn’t exist on the local system.
The remote location is the remote Dali (use the --daliip=<daliIP> parameter to specify the remote Dali)
You can also use ECL Watch.
```

9. **How can I start a cloud cluster? (akin to the old Virtual Box image)?**

**Answer**:
```
Can use Docker Desktop, or Azure or any cloud provider and install the HPCC Systems Cloud native helm
charts
```

10. **How can I show the ECL queries that are published to a given Roxie?**

**Answer**:
```
Can use WUListQueries
For example:
https://[eclwatch]:18010/WsWorkunits/WUListQueries.json?ver_=1.86&ClusterName=roxie&CheckAllNodes=0
```

11. **I set up persistent storage on my containerized HPCC Systems, and now it won't start. Why?**

**Answer**:
```
One possible reason may be that all of the required storage directories are not present. The directories for ~/
hpccdata/dalistorage, hpcc-data, debug, queries, sasha, and dropzone are all required to exist or your cluster may not start.
```

12. **Are there any new methods available to work with queries?**

**Answer**:
```
Yes. There is a new method available ServiceQuery.
https://[eclwatch]:18010/WsResources/ServiceQuery?ver_=1.01&
For example Roxie Queries:
https://[eclwatch]:18010/WsResources/ServiceQuery?ver_=1.01&Type=roxie
or WsECL (eclqueries)
https://[eclwatch]:18010/WsResources/ServiceQuery?ver_=1.01&Type=eclqueries
```