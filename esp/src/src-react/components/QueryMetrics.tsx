import * as React from "react";
import { Pivot, PivotItem } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { useQuerySnapshots } from "../hooks/query";
import { pivotItemStyle } from "../layouts/pivot";
import { pushUrl } from "../util/history";
import { Metrics } from "./Metrics";

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
    tab = wuid,
    selection
}) => {

    const [snapshots, _refresh] = useQuerySnapshots(querySet, queryId);

    return <SizeMe monitorHeight>{({ size }) =>
        <Pivot overflowBehavior="menu" style={{ height: "100%" }}
            selectedKey={tab}
            onLinkClick={evt => {
                pushUrl(`/queries/${querySet}/${queryId}/metrics/${evt.props.itemKey}`);
            }}>
            {
                (wuid && snapshots) ?
                    <PivotItem headerText={nlsHPCC.Compilation} itemKey={wuid} style={pivotItemStyle(size)} >
                        <Metrics wuid={wuid} parentUrl={`/queries/${querySet}/${queryId}/metrics/${wuid}`} selection={selection} />
                    </PivotItem> :
                    undefined
            }
            {
                (wuid && snapshots) ?
                    snapshots?.filter(snapshot => snapshot.Wuid !== wuid).map(snapshot => {
                        return <PivotItem headerText={snapshot.Wuid} itemKey={snapshot.Wuid} style={pivotItemStyle(size)} >
                            <Metrics wuid={snapshot.Wuid} parentUrl={`/queries/${querySet}/${queryId}/metrics/${snapshot.Wuid}`} selection={selection} />
                        </PivotItem>;
                    }) :
                    undefined
            }
        </Pivot>
    }</SizeMe>;
};