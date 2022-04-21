DatabricksClusters 
| where ActionName == 'startResult' and TimeGenerated > ago(14d)
| extend clusterinfo=parse_json(RequestParams)
| project
    tostring(clusterinfo.clusterId),
    TimeGenerated,
    ActionName,
    tostring(clusterinfo.clusterName)
| summarize max(TimeGenerated), max(ActionName) by clusterinfo_clusterId, tostring(clusterinfo_clusterName)
| as clusterstart
| join kind=leftouter (DatabricksClusters 
    | where ActionName == 'deleteResult' and TimeGenerated > ago(14d)
    | extend clusterinfo=parse_json(RequestParams)
    | project
        tostring(clusterinfo.clusterId),
        TimeGenerated,
        ActionName,
        tostring(clusterinfo.clusterName)
    | summarize max(TimeGenerated), max(ActionName) by clusterinfo_clusterId, tostring(clusterinfo_clusterName)
    ) 
    on $left.clusterinfo_clusterId == $right.clusterinfo_clusterId
| where max_TimeGenerated > max_TimeGenerated1
| project
    max_TimeGenerated,
    clusterinfo_clusterName,
    datetime_diff('hour', now(), max_TimeGenerated) 
| where datetime_diff('hour', now(), max_TimeGenerated) > 12
