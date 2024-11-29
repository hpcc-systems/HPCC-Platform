import * as React from "react";
import { useConst } from "@fluentui/react-hooks";
import { Palette } from "@hpcc-js/common";
import { ECLEditor } from "@hpcc-js/codemirror";
import { useUserTheme } from "../hooks/theme";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";

const palette = Palette.rainbow("YlOrRd");

interface ECLArchiveProps {
    ecl?: string;
    readonly?: boolean;
    markers?: { lineNum: number, label: string }[];
}

export const ECLArchiveEditor: React.FunctionComponent<ECLArchiveProps> = ({
    ecl = "",
    readonly = true,
    markers = []
}) => {
    const { isDark } = useUserTheme();

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
    }, [ecl, editor, isDark, readonly]);

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
