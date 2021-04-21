import * as React from "react";
import { VerticalDivider } from "@fluentui/react";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";

export const ShortVerticalDivider = () => <VerticalDivider styles={{ divider: { paddingTop: "20%", height: "60%" } }} />;

export function createCopyDownloadSelection(grid, selection: any, filename: string) {
    return [{
        key: "copy", text: nlsHPCC.CopySelectionToClipboard, disabled: !selection.length || !navigator?.clipboard?.writeText, iconOnly: true, iconProps: { iconName: "Copy" },
        onClick: () => {
            const tsv = Utility.formatAsDelim(grid, selection, "\t");
            navigator?.clipboard?.writeText(tsv);
        }
    },
    {
        key: "download", text: nlsHPCC.DownloadSelectionAsCSV, disabled: !selection.length, iconOnly: true, iconProps: { iconName: "Download" },
        onClick: () => {
            const csv = Utility.formatAsDelim(grid, selection, ",");
            Utility.downloadText(csv, filename);
        }
    }];
}

