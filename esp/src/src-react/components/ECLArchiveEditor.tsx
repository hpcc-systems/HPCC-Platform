import * as React from "react";
import { makeStyles } from "@fluentui/react-components";
import { useConst } from "@fluentui/react-hooks";
import { Palette } from "@hpcc-js/common";
import { ECLEditor } from "@hpcc-js/codemirror";
import { useUserTheme } from "../hooks/theme";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";

const palette = Palette.rainbow("YlOrRd");

const useStyles = makeStyles({
    eclarchiveHighlightLine: {
        backgroundColor: "rgba(130, 198, 235, 0.4) !important",
    },
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
    const styles = useStyles();

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
                editor.addLineClass({ lineNum: lineNum - 1, cssClass: styles.eclarchiveHighlightLine });
                setPrevSelLine(lineNum);
                window.clearTimeout(t);
            }, 100);
        }
    }, [ecl, editor, isDark, readonly, lineNum]);

    React.useEffect(() => {
        // Remove previous highlight if it exists
        if (prevSelLine >= 0 && lineNum !== prevSelLine) {
            editor.removeLineClass({ lineNum: prevSelLine - 1, cssClass: styles.eclarchiveHighlightLine });
        }
    }, [editor, lineNum, prevSelLine]);

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
