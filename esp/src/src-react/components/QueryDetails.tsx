import * as React from "react";
import { Pivot, PivotItem } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import * as ESPQuery from "src/ESPQuery";
import { pivotItemStyle, usePivotItemDisable } from "../layouts/pivot";
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

interface QueryDetailsProps {
    querySet: string;
    queryId: string;
    tab?: string;
    metricsTab?: string;
    testTab?: string;
    state?: string;
}

export const QueryDetails: React.FunctionComponent<QueryDetailsProps> = ({
    querySet,
    queryId,
    tab = "summary",
    metricsTab,
    testTab = "form",
    state
}) => {

    const [query, setQuery] = React.useState<any>();
    const [wuid, setWuid] = React.useState<string>("");
    const [logicalFileCount, setLogicalFileCount] = React.useState<number>(0);
    const [superFileCount, setSuperFileCount] = React.useState<number>(0);
    const [libsUsedCount, setLibsUsedCount] = React.useState<number>(0);
    const wuidDisable = usePivotItemDisable(wuid === "");

    React.useEffect(() => {
        setQuery(ESPQuery.Get(querySet, queryId));
    }, [setQuery, queryId, querySet]);

    React.useEffect(() => {
        query?.getDetails().then(({ WUQueryDetailsResponse }) => {
            setWuid(query.Wuid);
            setLogicalFileCount(query.LogicalFiles?.Item?.length);
            setSuperFileCount(query.SuperFiles?.SuperFile?.length);
            setLibsUsedCount(query.LibrariesUsed?.Item?.length);
        });
    }, [query, setLogicalFileCount, setSuperFileCount, setLibsUsedCount]);

    return <SizeMe monitorHeight>{({ size }) =>
        <Pivot
            overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab}
            onLinkClick={evt => {
                switch (evt.props.itemKey) {
                    case "testPages":
                        pushUrl(`/queries/${querySet}/${queryId}/testPages/${testTab}`);
                        break;
                    default:
                        pushUrl(`/queries/${querySet}/${queryId}/${evt.props.itemKey}`);
                        break;
                }
            }}
        >
            <PivotItem headerText={queryId} itemKey="summary" style={pivotItemStyle(size)} >
                <QuerySummary queryId={queryId} querySet={querySet} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Errors} itemKey="errors" style={pivotItemStyle(size, 0)}>
                <QueryErrors queryId={queryId} querySet={querySet} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.LogicalFiles} itemKey="logicalFiles" itemCount={logicalFileCount} style={pivotItemStyle(size, 0)}>
                <QueryLogicalFiles queryId={queryId} querySet={querySet} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.SuperFiles} itemKey="superfiles" itemCount={superFileCount} style={pivotItemStyle(size, 0)}>
                <QuerySuperFiles queryId={queryId} querySet={querySet} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.LibrariesUsed} itemKey="librariesUsed" itemCount={libsUsedCount} style={pivotItemStyle(size, 0)}>
                <QueryLibrariesUsed queryId={queryId} querySet={querySet} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.SummaryStatistics} itemKey="summaryStatistics" style={pivotItemStyle(size, 0)}>
                <QuerySummaryStats queryId={queryId} querySet={querySet} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Metrics} itemKey="metrics" style={pivotItemStyle(size, 0)}>
                <QueryMetrics wuid={query?.Wuid} queryId={queryId} querySet={querySet} tab={metricsTab} selection={state} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Resources} itemKey="resources" headerButtonProps={wuidDisable} style={pivotItemStyle(size, 0)}>
                {wuid && <Resources wuid={wuid} />}
            </PivotItem>
            <PivotItem headerText={nlsHPCC.TestPages} itemKey="testPages" style={pivotItemStyle(size, 0)}>
                <QueryTests queryId={queryId} querySet={querySet} tab={testTab} />
            </PivotItem>
        </Pivot>
    }</SizeMe>;
};
