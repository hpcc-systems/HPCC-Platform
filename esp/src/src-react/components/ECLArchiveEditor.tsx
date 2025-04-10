import * as React from "react";
import { mergeStyles } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { Palette } from "@hpcc-js/common";
import { ECLEditor } from "@hpcc-js/codemirror";
import { useUserTheme } from "../hooks/theme";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";

const palette = Palette.rainbow("YlOrRd");

// Apply global styles for codemirror line highlighting
mergeStyles({
    selectors: {
        ":global(.eclarchive-highlight-line)": {
            backgroundColor: "rgba(130, 198, 235, 0.4) !important",
        },
        ":global(.cm-s-darcula .eclarchive-highlight-line)": {
            backgroundColor: "rgba(55, 115, 148, 0.4) !important",
        }
    }
});

interface ECLArchiveProps {
    ecl?: string;
    readonly?: boolean;
    markers?: { lineNum: number, label: string }[];
    lineNum?: number;
}

export const ECLArchiveEditor: React.FunctionComponent<ECLArchiveProps> = ({
    ecl = "",
    readonly = true,
    markers = [],
    lineNum
}) => {
    const { isDark } = useUserTheme();
    const [prevSelLine, setPrevSelLine] = React.useState(-1);

    const editor = useConst(() =>
        new ECLEditor()
            .readOnly(true)
    );

    React.useEffect(() => {
        editor
            ?.text(ecl)
            ?.readOnly(readonly)
            ?.option("theme", isDark ? "darcula" : "default")
            ?.lazyRender()
            ;

        if (lineNum !== undefined) {
            editor?.setCursor(lineNum - 1, 1, true);
            const t = window.setTimeout(() => {
                const cmInstance = (editor as any)?._codemirror;
                if (cmInstance) {
                    const lineHandle = cmInstance.getLineHandle(lineNum - 1);
                    setPrevSelLine(lineNum - 1);
                    if (lineHandle) {
                        cmInstance.addLineClass(lineHandle, "background", "eclarchive-highlight-line");
                    }
                }
                window.clearTimeout(t);
            }, 100);
        }
    }, [ecl, editor, isDark, readonly, lineNum]);

    React.useEffect(() => {
        const cmInstance = (editor as any)?._codemirror;
        if (cmInstance) {
            // Remove any existing highlight classes
            const doc = cmInstance.getDoc();
            if (prevSelLine) {
                const lineHandle = doc.getLineHandle(prevSelLine);
                if (lineHandle && lineHandle.bgClass && lineHandle.bgClass.includes("eclarchive-highlight-line")) {
                    cmInstance.removeLineClass(lineHandle, "background", "eclarchive-highlight-line");
                }
            }
        }
    }, [editor, prevSelLine]);

    React.useEffect(() => {
        const fontFamily = "Verdana";
        const fontSize = 12;
        const maxLabelWidth = Math.max(
            ...markers.map(marker => {
                const color = palette(+marker.label, 0, 100);
                editor?.addGutterMarker(+marker.lineNum - 1, marker.label, color, fontFamily, `${fontSize}px`);
                return editor?.textSize(marker.label, fontFamily, fontSize)?.width ?? 0;
            })
        );
        editor
            ?.gutterMarkerWidth(maxLabelWidth + 6)
            ?.lazyRender()
            ;
    }, [editor, markers]);

    return <AutosizeHpccJSComponent widget={editor}></AutosizeHpccJSComponent>;
};
