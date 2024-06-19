package framework.config;

import framework.model.URLMapping;
import framework.utility.Common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class URLConfig {

    public static final String LOCAL_SELENIUM_SERVER = "http://localhost:4444/wd/hub";
    public static final String LOCAL_IP = "http://192.168.0.221:8010/";
    public static final String GITHUB_ACTION_IP = "http://127.0.0.1:8010/";

    public static final String NAV_ACTIVITIES = "Activities";
    public static final String NAV_ECL = "ECL";
    public static final String NAV_FILES = "Files";
    public static final String NAV_PUBLISHED_QUERIES = "Published Queries";
    public static final String NAV_OPERATIONS = "Operations";
    public static final String TAB_ACTIVITIES_ACTIVITIES = "Activities";
    public static final String TAB_ACTIVITIES_EVENT_SCHEDULER = "Event Scheduler";
    public static final String TAB_ECL_WORKUNITS = "Workunits";
    public static final String TAB_ECL_PLAYGROUND = "Playground";
    public static final String TAB_FILES_LOGICAL_FILES = "Logical Files";
    public static final String TAB_FILES_LANDING_ZONES = "Landing Zones";
    public static final String TAB_FILES_WORKUNITS = "Workunits";
    public static final String TAB_FILES_XREF = "XRef (L)";
    public static final String TAB_PUBLISHED_QUERIES_QUERIES = "Queries";
    public static final String TAB_PUBLISHED_QUERIES_PACKAGE_MAPS = "Package Maps";
    public static final String TAB_OPERATIONS_TOPOLOGY = "Topology (L)";
    public static final String TAB_OPERATIONS_DISK_USAGE = "Disk Usage (L)";
    public static final String TAB_OPERATIONS_TARGET_CLUSTERS = "Target Clusters (L)";
    public static final String TAB_OPERATIONS_CLUSTER_PROCESSES = "Cluster Processes (L)";
    public static final String TAB_OPERATIONS_SYSTEM_SERVERS = "System Servers (L)";
    public static final String TAB_OPERATIONS_SECURITY = "Security (L)";
    public static final String TAB_OPERATIONS_DYNAMIC_ESDL = "Dynamic ESDL (L)";

    public static final String[] navNamesArray = {NAV_ACTIVITIES, NAV_ECL, NAV_FILES, NAV_PUBLISHED_QUERIES, NAV_OPERATIONS};
    public static final Map<String, List<String>> tabsListMap = Map.of(
            NAV_ACTIVITIES, List.of(TAB_ACTIVITIES_ACTIVITIES, TAB_ACTIVITIES_EVENT_SCHEDULER),
            NAV_ECL, List.of(TAB_ECL_WORKUNITS, TAB_ECL_PLAYGROUND),
            NAV_FILES, List.of(TAB_FILES_LOGICAL_FILES, TAB_FILES_LANDING_ZONES, TAB_FILES_WORKUNITS, TAB_FILES_XREF),
            NAV_PUBLISHED_QUERIES, List.of(TAB_PUBLISHED_QUERIES_QUERIES, TAB_PUBLISHED_QUERIES_PACKAGE_MAPS),
            NAV_OPERATIONS, List.of(TAB_OPERATIONS_TOPOLOGY, TAB_OPERATIONS_DISK_USAGE, TAB_OPERATIONS_TARGET_CLUSTERS,
                    TAB_OPERATIONS_CLUSTER_PROCESSES, TAB_OPERATIONS_SYSTEM_SERVERS, TAB_OPERATIONS_SECURITY, TAB_OPERATIONS_DYNAMIC_ESDL)
    );

    public static final HashMap<String, URLMapping> urlMap = new HashMap<>();

    static {
        urlMap.put(NAV_ACTIVITIES, new URLMapping(NAV_ACTIVITIES, Common.getIP()));
    }
}
