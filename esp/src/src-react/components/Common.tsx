import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";

export function createCopyDownloadSelection(columns, selection: any, filename: string) {
    return [{
        key: "copy", text: nlsHPCC.CopySelectionToClipboard, disabled: !selection.length || !navigator?.clipboard?.writeText, iconOnly: true, iconProps: { iconName: "Copy" },
        onClick: () => {
            const tsv = Utility.formatAsDelim(columns, selection, "\t");
            navigator?.clipboard?.writeText(tsv);
        }
    },
    {
        key: "download", text: nlsHPCC.DownloadSelectionAsCSV, disabled: !selection.length, iconOnly: true, iconProps: { iconName: "Download" },
        onClick: () => {
            const csv = Utility.formatAsDelim(columns, selection, ",");
            Utility.downloadCSV(csv, filename);
        }
    }];
}

