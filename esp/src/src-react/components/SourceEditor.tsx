import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { XMLEditor } from "@hpcc-js/codemirror";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { useWorkunitXML } from "../hooks/Workunit";
import { ShortVerticalDivider } from "./Common";

interface SourceEditorProps {
    text: string;
    readonly?: boolean;
}

export const XMLSourceEditor: React.FunctionComponent<SourceEditorProps> = ({
    text = "",
    readonly = false
}) => {

    //  Command Bar  ---
    const buttons: ICommandBarItemProps[] = [
        {
            key: "copy", text: nlsHPCC.Copy, iconProps: { iconName: "Copy" },
            onClick: () => {
                navigator?.clipboard?.writeText(text);
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ];

    const editor = useConst(new XMLEditor());
    React.useEffect(() => {
        editor
            .text(text)
            .readOnly(readonly)
            .lazyRender()
            ;

    }, [editor, readonly, text]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} />}
        main={
            <AutosizeHpccJSComponent widget={editor} padding={4} />
        }
    />;
};

export interface WUXMLSourceEditorProps {
    wuid: string;
}

export const WUXMLSourceEditor: React.FunctionComponent<WUXMLSourceEditorProps> = ({
    wuid
}) => {

    const [xml] = useWorkunitXML(wuid);

    return <XMLSourceEditor text={xml} readonly={true} />;
};
