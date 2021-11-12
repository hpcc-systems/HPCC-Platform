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

    React.useEffect(() => {
        setQuery(ESPQuery.Get(querySet, queryId));
    }, [setQuery, queryId, querySet]);

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
            <PivotItem headerText={nlsHPCC.LogicalFiles} itemKey="logicalFiles" itemCount={query?.LogicalFiles?.Item?.length || 0} style={pivotItemStyle(size, 0)}>
                <QueryLogicalFiles queryId={queryId} querySet={querySet} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.SuperFiles} itemKey="superfiles" itemCount={query?.SuperFiles?.SuperFile.length || 0} style={pivotItemStyle(size, 0)}>
                <QuerySuperFiles queryId={queryId} querySet={querySet} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.LibrariesUsed} itemKey="librariesUsed" itemCount={query?.LibrariesUsed?.Item?.length || 0} style={pivotItemStyle(size, 0)}>
                <QueryLibrariesUsed queryId={queryId} querySet={querySet} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.SummaryStatistics} itemKey="summaryStatistics" style={pivotItemStyle(size, 0)}>
                <QuerySummaryStats queryId={queryId} querySet={querySet} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Graphs} itemKey="graphs" itemCount={query?.WUGraphs?.ECLGraph?.length || 0} style={pivotItemStyle(size, 0)}>
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
