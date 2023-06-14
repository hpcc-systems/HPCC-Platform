import * as React from "react";
import { Pivot, PivotItem } from "@fluentui/react";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import * as ESPQuery from "src/ESPQuery";
import { pivotItemStyle } from "../layouts/pivot";
import { pushUrl } from "../util/history";
import { QueryErrors } from "./QueryErrors";
import { QueryGraphs } from "./QueryGraphs";
import { QueryLibrariesUsed } from "./QueryLibrariesUsed";
import { QueryLogicalFiles } from "./QueryLogicalFiles";
import { QuerySummary } from "./QuerySummary";
import { QuerySummaryStats } from "./QuerySummaryStats";
import { QuerySuperFiles } from "./QuerySuperFiles";
import { QueryTests } from "./QueryTests";
import { Resources } from "./Resources";

interface QueryDetailsProps {
    querySet: string;
    queryId: string;
    tab?: string;
    testTab?: string;
}

export const QueryDetails: React.FunctionComponent<QueryDetailsProps> = ({
    querySet,
    queryId,
    tab = "summary",
    testTab = "form"
}) => {

    const [query, setQuery] = React.useState<any>();
    const [logicalFileCount, setLogicalFileCount] = React.useState<number>(0);
    const [superFileCount, setSuperFileCount] = React.useState<number>(0);
    const [libsUsedCount, setLibsUsedCount] = React.useState<number>(0);
    const [graphCount, setGraphCount] = React.useState<number>(0);

    React.useEffect(() => {
        setQuery(ESPQuery.Get(querySet, queryId));
    }, [setQuery, queryId, querySet]);

    React.useEffect(() => {
        query?.getDetails().then(({ WUQueryDetailsResponse }) => {
            setLogicalFileCount(query.LogicalFiles.Item.length);
            setSuperFileCount(query.SuperFiles.SuperFile.length);
            setLibsUsedCount(query.LibrariesUsed.Item.length);
            setGraphCount(query.WUGraphs.ECLGraph.length);
        });
    }, [query, setLogicalFileCount, setSuperFileCount, setLibsUsedCount, setGraphCount]);

    return <SizeMe monitorHeight>{({ size }) =>
        <Pivot
            overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab}
            onLinkClick={evt => {
                switch (evt.props.itemKey) {
                    case "workunit":
                        pushUrl(`/workunits/${query?.Wuid}`);
                        break;
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
            <PivotItem headerText={nlsHPCC.Graphs} itemKey="graphs" itemCount={graphCount} style={pivotItemStyle(size, 0)}>
                <QueryGraphs queryId={queryId} querySet={querySet} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Resources} itemKey="resources" style={pivotItemStyle(size, 0)}>
                <Resources wuid={query?.Wuid} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.TestPages} itemKey="testPages" style={pivotItemStyle(size, 0)}>
                <QueryTests queryId={queryId} querySet={querySet} tab={testTab} />
            </PivotItem>
            <PivotItem headerText={query?.Wuid} itemKey="workunit"></PivotItem>
        </Pivot>
    }</SizeMe>;
};
