import * as React from "react";
import { Tab, TabList, SelectTabData, SelectTabEvent, makeStyles } from "@fluentui/react-components";
import { scopedLogger } from "@hpcc-js/util";
import { SizeMe } from "../layouts/SizeMe";
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

const useStyles = makeStyles({
    container: {
        height: "100%",
        position: "relative"
    }
});

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
                    if (Array.isArray(xrefNodes)) {
                        xrefNodes.filter(node => node.Name === name).forEach(n => {
                            setLastRun(n.Modified);
                            setStatus(n.Status);
                        });
                    } else {
                        setLastRun(xrefNodes.Modified);
                        setStatus(xrefNodes.Status);
                    }
                }
            })
            .catch(err => logger.error(err))
            ;
    }, [name]);

    const onTabSelect = React.useCallback((_: SelectTabEvent, data: SelectTabData) => {
        pushUrl(`/xref/${name}/${data.value as string}`);
    }, [name]);

    const styles = useStyles();

    return <>
        <SizeMe>{({ size }) =>
            <div className={styles.container}>
                <TabList selectedValue={tab} onTabSelect={onTabSelect} size="medium">
                    <Tab value="summary">{nlsHPCC.Summary}</Tab>
                    <Tab value="foundFiles">{nlsHPCC.FoundFile}</Tab>
                    <Tab value="orphanFiles">{nlsHPCC.OrphanFile}</Tab>
                    <Tab value="lostFiles">{nlsHPCC.LostFile}</Tab>
                    <Tab value="directories">{nlsHPCC.Directories}</Tab>
                    <Tab value="errors">{nlsHPCC.ErrorWarnings}</Tab>
                </TabList>
                {tab === "summary" &&
                    <div style={pivotItemStyle(size)}>
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
                    </div>
                }
                {tab === "foundFiles" &&
                    <div style={pivotItemStyle(size)}>
                        <XrefFoundFiles name={name} />
                    </div>
                }
                {tab === "orphanFiles" &&
                    <div style={pivotItemStyle(size)}>
                        <XrefOrphanFiles name={name} />
                    </div>
                }
                {tab === "lostFiles" &&
                    <div style={pivotItemStyle(size)}>
                        <XrefLostFiles name={name} />
                    </div>
                }
                {tab === "directories" &&
                    <div style={pivotItemStyle(size)}>
                        <XrefDirectories name={name} />
                    </div>
                }
                {tab === "errors" &&
                    <div style={pivotItemStyle(size)}>
                        <XrefErrors name={name} />
                    </div>
                }
            </div>
        }</SizeMe>
    </>;

};