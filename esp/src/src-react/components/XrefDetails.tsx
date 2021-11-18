import * as React from "react";
import { Pivot, PivotItem } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { SizeMe } from "react-sizeme";
import { pivotItemStyle } from "../layouts/pivot";
import * as WsDFUXref from "src/WsDFUXref";
import { XrefFoundFiles } from "./XrefFoundFiles";
import { XrefLostFiles } from "./XrefLostFiles";
import { XrefOrphanFiles } from "./XrefOrphanFiles";
import { XrefDirectories } from "./XrefDirectories";
import { XrefErrors } from "./XrefErrors";
import { pushUrl } from "../util/history";
import nlsHPCC from "src/nlsHPCC";
import * as Utility from "src/Utility";

const logger = scopedLogger("src-react/components/XrefDetails.tsx");

interface XrefDetailsProps {
    name: string;
    tab?: string;
}

export const XrefDetails: React.FunctionComponent<XrefDetailsProps> = ({
    name,
    tab = "summary"
}) => {

    const [lastRun, setLastRun] = React.useState("");
    const [status, setStatus] = React.useState("");

    React.useEffect(() => {
        WsDFUXref.WUGetXref({
            request: {}
        })
            .then(({ DFUXRefListResponse }) => {
                const xrefNodes = DFUXRefListResponse?.DFUXRefListResult?.XRefNode;
                if (xrefNodes) {
                    xrefNodes.filter(node => node.Name === name).forEach(n => {
                        setLastRun(n.Modified);
                        setStatus(n.Status);
                    });
                }
            })
            .catch(err => logger.error(err))
            ;
    }, [name]);

    return <>
        <SizeMe monitorHeight>{({ size }) =>
            <Pivot
                overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab}
                onLinkClick={evt => pushUrl(`/xref/${name}/${evt.props.itemKey}`)}
            >
                <PivotItem headerText={nlsHPCC.Summary} itemKey="summary" style={pivotItemStyle(size)}>

                    <h2>
                        <img src={Utility.getImageURL("cluster.png")} />&nbsp;<span className="bold">{name}</span>
                    </h2>
                    <table style={{ padding: 4 }}>
                        <tbody>
                            <tr>
                                <td style={{ fontWeight: "bold" }}>{nlsHPCC.LastRun}</td>
                                <td style={{ width: "80%", paddingLeft: 8 }}>{lastRun}</td>
                            </tr>
                            <tr>
                                <td style={{ fontWeight: "bold" }}>{nlsHPCC.LastMessage}</td>
                                <td style={{ width: "80%", paddingLeft: 8 }}>{status}</td>
                            </tr>
                            <tr>
                                <td style={{ fontWeight: "bold" }}>{nlsHPCC.FoundFile}</td>
                                <td style={{ width: "80%", paddingLeft: 8 }}>{nlsHPCC.FoundFileMessage}</td>
                            </tr>
                            <tr>
                                <td style={{ fontWeight: "bold" }}>{nlsHPCC.OrphanFile2}</td>
                                <td style={{ width: "80%", paddingLeft: 8 }}>{nlsHPCC.OrphanMessage}</td>
                            </tr>
                            <tr>
                                <td style={{ fontWeight: "bold" }}>{nlsHPCC.LostFile}</td>
                                <td style={{ width: "80%", paddingLeft: 8 }}>{nlsHPCC.LostFileMessage}</td>
                            </tr>
                        </tbody>
                    </table>
                </PivotItem>
                <PivotItem headerText={nlsHPCC.FoundFile} itemKey="foundFiles" style={pivotItemStyle(size)}>
                    <XrefFoundFiles name={name} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.OrphanFile} itemKey="orphanFiles" style={pivotItemStyle(size)}>
                    <XrefOrphanFiles name={name} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.LostFile} itemKey="lostFiles" style={pivotItemStyle(size)}>
                    <XrefLostFiles name={name} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.Directories} itemKey="directories" style={pivotItemStyle(size)}>
                    <XrefDirectories name={name} />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.ErrorWarnings} itemKey="errors" style={pivotItemStyle(size)}>
                    <XrefErrors name={name} />
                </PivotItem>
            </Pivot>
        }</SizeMe>
    </>;

};