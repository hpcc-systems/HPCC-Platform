import * as React from "react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { useQuerySnapshots } from "../hooks/query";
import { pushUrl } from "../util/history";
import { Metrics } from "./Metrics";
import { DelayLoadedPanel, OverflowTabList, TabInfo } from "./controls/TabbedPanes/index";

interface QueryMetricsProps {
    wuid: string;
    querySet: string;
    queryId: string;
    tab?: string;
    selection?: string;
}

export const QueryMetrics: React.FunctionComponent<QueryMetricsProps> = ({
    wuid,
    querySet,
    queryId,
    tab = "statistics",
    selection
}) => {

    const [snapshots, _refresh] = useQuerySnapshots(querySet, queryId);

    const onTabSelect = React.useCallback((tab: TabInfo) => {
        pushUrl(tab.__state ?? `/queries/${querySet}/${queryId}/metrics/${tab.id}`);
    }, [queryId, querySet]);

    const tabs = React.useMemo((): TabInfo[] => {
        const retVal = [{
            id: "statistics",
            label: nlsHPCC.Statistics,
        }];
        snapshots?.filter(snapshot => snapshot.Wuid !== wuid).forEach(snapshot => {
            retVal.push({
                id: snapshot.Wuid,
                label: snapshot.Wuid
            });
        });
        return retVal;
    }, [snapshots, wuid]);

    return <SizeMe monitorHeight>{({ size }) =>
        <div style={{ height: "100%" }}>
            <OverflowTabList tabs={tabs} selected={tab} onTabSelect={onTabSelect} size="medium" />
            <DelayLoadedPanel visible={tab === "statistics"} size={size}>
                <Metrics wuid={wuid} querySet={querySet} queryId={queryId} parentUrl={`/queries/${querySet}/${queryId}/metrics/statistics`} selection={selection} />
            </DelayLoadedPanel>
            {
                snapshots?.filter(snapshot => snapshot.Wuid !== wuid).map(snapshot => {
                    return <DelayLoadedPanel visible={tab === snapshot.Wuid} size={size}>
                        <Metrics wuid={snapshot.Wuid} parentUrl={`/queries/${querySet}/${queryId}/metrics/${snapshot.Wuid}`} selection={selection} />
                    </DelayLoadedPanel>;
                })
            }
        </div>
    }</SizeMe>;
};