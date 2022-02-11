import * as React from "react";
import { Checkbox, ICheckboxStyles, ISpinButtonStyles, Label, SpinButton } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { Workunit } from "@hpcc-js/comms";
import nlsHPCC from "src/nlsHPCC";
import { useLogicalClusters } from "../hooks/platform";
import { TextSourceEditor } from "./SourceEditor";

const logger = scopedLogger("src-react/components/HexView.tsx");

interface HexViewProps {
    logicalFile: string;
}

const bufferLength = 16 * 1024;
const unknownChar = String.fromCharCode(8226);

const spinStyles: Partial<ISpinButtonStyles> = { root: { width: 100 }, spinButtonWrapper: { width: 50 } };
const checkboxStyles: Partial<ICheckboxStyles> = { root: { marginTop: 4 } };

const isCharPrintable = (char) => {
    const charCode = char.charCodeAt(0);
    if (charCode < 32) return false;
    else if (charCode >= 127 && charCode <= 159) return false;
    else if (charCode === 173) return false;
    else if (charCode > 255) return false;
    return true;
};

export const HexView: React.FunctionComponent<HexViewProps> = ({
    logicalFile
}) => {

    const [text, setText] = React.useState("");
    const [cachedResponse, setCachedResponse] = React.useState<any[]>([]);
    const [lineLength, setLineLength] = React.useState("16");
    const [showEbcdic, setShowEbcdic] = React.useState(false);

    const onLineLengthChange = React.useCallback((event: React.FormEvent<HTMLInputElement>, newValue?: string) => {
        if (newValue !== undefined) {
            setLineLength(newValue);
        }
    }, []);

    const query = React.useMemo(() => {
        return `data_layout := record
    data1 char;
end;
data_dataset := dataset('${logicalFile}', data_layout, thor);
analysis_layout := record
    data1 char;
    string1 str1;
    ebcdic string1 estr1;
end;
analysis_layout calcAnalysis(data_layout l) := transform
    self.char := l.char;
    self.str1 := transfer(l.char, string1);
    self.estr1 := transfer(l.char, string1);
end;
analysis_dataset := project(data_dataset, calcAnalysis(left));
choosen(analysis_dataset, ${bufferLength});`;
    }, [logicalFile]);

    const [, defaultCluster] = useLogicalClusters();

    React.useEffect(() => {
        if (!defaultCluster?.Name) return;
        Workunit.submit({ baseUrl: "" }, defaultCluster?.Name, query).then(wu => {
            wu.on("changed", function () {
                if (!wu.isComplete()) {
                    setText("..." + wu.State + "...");
                }
            });
            return wu.watchUntilComplete();
        }).then(function (wu) {
            return wu.fetchECLExceptions()
                .then(() => wu)
                .catch(err => wu);
        }).then(function (wu) {
            const exceptions = wu.Exceptions && wu.Exceptions.ECLException ? wu.Exceptions.ECLException : [];
            if (exceptions.length) {
                let msg = "";
                exceptions.map(exception => {
                    if (exception.Severity === "Error") {
                        if (msg) {
                            msg += "\n";
                        }
                        msg += exception.Message;
                    }
                });
                if (msg) {
                    logger.error({
                        Severity: "Error",
                        Source: "HexViewWidget.remoteRead",
                        Exceptions: [{ Source: wu.Wuid, Message: msg }]
                    });
                }
            }
            return wu.fetchResults().then(results => {
                return results.length ? results[0].fetchRows() : [];
            }).then(rows => {
                setCachedResponse(rows);
                return wu;
            });
        }).then(function (wu) {
            return wu.delete();
        });
    }, [defaultCluster?.Name, query]);

    React.useEffect(() => {
        const formatRow = (row, strRow, hexRow, length) => {
            if (row) {
                for (let i = row.length; i < 4; ++i) {
                    row = "0" + row;
                }
                for (let i = strRow.length; i < length; ++i) {
                    strRow += unknownChar;
                }
                return row + "  " + strRow + "  " + hexRow + "\n";
            }
            return "";
        };

        let doc = "";
        let row = "";
        let hexRow = "";
        let strRow = "";
        let charIdx = 0;
        const _lineLength = parseInt(lineLength, 10);

        cachedResponse.map((item, idx) => {
            if (idx >= _lineLength * 100) {
                return false;
            }
            if (idx % _lineLength === 0) {
                doc += formatRow(row, strRow, hexRow, _lineLength);
                row = "";
                hexRow = "";
                strRow = "";
                charIdx = 0;
                row = idx.toString(16);
            }
            if (charIdx % 8 === 0) {
                if (hexRow)
                    hexRow += " ";
            }
            if (hexRow)
                hexRow += " ";
            hexRow += item["char"];

            if (showEbcdic) {
                strRow += isCharPrintable(item.estr1) ? item.estr1 : unknownChar;
            } else {
                strRow += isCharPrintable(item.str1) ? item.str1 : unknownChar;
            }
            ++charIdx;
        });
        doc += formatRow(row, strRow, hexRow, _lineLength);
        setText(doc);
    }, [cachedResponse, lineLength, showEbcdic]);

    return <>
        <div style={{ display: "flex", padding: "8px", gap: "10px" }}>
            <Label>{nlsHPCC.Width}</Label>
            <SpinButton value={lineLength.toString()} min={1} onChange={onLineLengthChange} styles={spinStyles} />
            <Label>{nlsHPCC.EBCDIC}</Label>
            <Checkbox onChange={(ev, checked) => setShowEbcdic(checked)} styles={checkboxStyles} />
        </div>
        <TextSourceEditor text={text} readonly={true} />
    </>;

};