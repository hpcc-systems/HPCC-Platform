import * as React from "react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import * as ESPQuery from "src/ESPQuery";
import { pushUrl } from "../util/history";
import { QueryErrors } from "./QueryErrors";
import { QueryLibrariesUsed } from "./QueryLibrariesUsed";
import { QueryLogicalFiles } from "./QueryLogicalFiles";
import { QuerySummary } from "./QuerySummary";
import { QuerySummaryStats } from "./QuerySummaryStats";
import { QuerySuperFiles } from "./QuerySuperFiles";
import { QueryTests } from "./QueryTests";
import { Resources } from "./Resources";
import { QueryMetrics } from "./QueryMetrics";
import { DelayLoadedPanel, OverflowTabList, TabInfo } from "./controls/TabbedPanes/index";

interface QueryDetailsProps {
    querySet: string;
    queryId: string;
    tab?: string;
    state?: { metricsTab?: string, metricsState?: string, testTab?: string };
    queryParams?: { metricsSelection?: string };
}

export const QueryDetails: React.FunctionComponent<QueryDetailsProps> = ({
    querySet,
    queryId,
    tab = "summary",
    state = {},
    queryParams = {}
}) => {
    state.testTab = state.testTab ?? "Form";

    const [query, setQuery] = React.useState<any>();
    const [wuid, setWuid] = React.useState<string>("");
    const [logicalFileCount, setLogicalFileCount] = React.useState<number>(0);
    const [superFileCount, setSuperFileCount] = React.useState<number>(0);
    const [libsUsedCount, setLibsUsedCount] = React.useState<number>(0);
    const [suspended, setSuspended] = React.useState(false);
    const [activated, setActivated] = React.useState(false);

    React.useEffect(() => {
        setQuery(ESPQuery.Get(querySet, queryId));
    }, [setQuery, queryId, querySet]);

    React.useEffect(() => {
        query?.getDetails().then(() => {
            setWuid(query.Wuid);
            setLogicalFileCount(query.LogicalFiles?.Item?.length);
            setSuperFileCount(query.SuperFiles?.SuperFile?.length);
            setLibsUsedCount(query.LibrariesUsed?.Item?.length);
            setActivated(query.Activated);
            setSuspended(query.Suspended);
        });
    }, [query]);

    const onTabSelect = React.useCallback((tab: TabInfo) => {
        switch (tab.id) {
            case "testPages":
                pushUrl(tab.__state ?? `/queries/${querySet}/${queryId}/testPages/${state.testTab}`);
                break;
            default:
                pushUrl(tab.__state ?? `/queries/${querySet}/${queryId}/${tab.id}`);
                break;
        }
    }, [queryId, querySet, state.testTab]);

    const tabs = React.useMemo((): TabInfo[] => {
        return [{
            id: "summary",
            label: nlsHPCC.Summary,
        }, {
            id: "errors",
            label: nlsHPCC.Errors,
        }, {
            id: "logicalFiles",
            label: nlsHPCC.LogicalFiles,
            count: logicalFileCount
        }, {
            id: "superfiles",
            label: nlsHPCC.SuperFiles,
            count: superFileCount
        }, {
            id: "librariesUsed",
            label: nlsHPCC.LibrariesUsed,
            count: libsUsedCount
        }, {
            id: "summaryStatistics",
            label: nlsHPCC.SummaryStatistics,
        }, {
            id: "metrics",
            label: nlsHPCC.Metrics,
        }, {
            id: "resources",
            label: nlsHPCC.Resources,
            disabled: wuid === ""
        }, {
            id: "testPages",
            label: nlsHPCC.TestPages,
        }];
    }, [libsUsedCount, logicalFileCount, superFileCount, wuid]);

    return <SizeMe monitorHeight>{({ size }) =>
        <div style={{ height: "100%" }}>
            <OverflowTabList tabs={tabs} selected={tab} onTabSelect={onTabSelect} size="medium" />
            <DelayLoadedPanel visible={tab === "summary"} size={size}>
                <QuerySummary queryId={queryId} querySet={querySet} isSuspended={suspended} isActivated={activated} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "errors"} size={size}>
                <QueryErrors queryId={queryId} querySet={querySet} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "logicalFiles"} size={size}>
                <QueryLogicalFiles queryId={queryId} querySet={querySet} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "superfiles"} size={size}>
                <QuerySuperFiles queryId={queryId} querySet={querySet} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "librariesUsed"} size={size}>
                <QueryLibrariesUsed queryId={queryId} querySet={querySet} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "summaryStatistics"} size={size}>
                <QuerySummaryStats queryId={queryId} querySet={querySet} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "metrics"} size={size}>
                <QueryMetrics wuid={query?.Wuid} queryId={queryId} querySet={querySet} tab={state.metricsTab} selection={queryParams.metricsSelection} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "resources"} size={size}>
                <Resources wuid={wuid} />
            </DelayLoadedPanel>
            <DelayLoadedPanel visible={tab === "testPages"} size={size}>
                <QueryTests queryId={queryId} querySet={querySet} tab={state.testTab} />
            </DelayLoadedPanel>
        </div>
    }</SizeMe>;
};
