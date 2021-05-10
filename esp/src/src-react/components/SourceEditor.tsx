import * as React from "react";
import { CommandBar, ContextualMenuItemType, getTheme, ICommandBarItemProps } from "@fluentui/react";
import { useConst, useOnEvent } from "@fluentui/react-hooks";
import { Editor, XMLEditor } from "@hpcc-js/codemirror";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { useWorkunitXML } from "../hooks/Workunit";
import { darkTheme } from "../themes";
import { ShortVerticalDivider } from "./Common";
import "codemirror/theme/darcula.css";

interface SourceEditorProps {
    text?: string;
    readonly?: boolean;
    mode?: "ecl" | "xml" | "text";
}

const SourceEditor: React.FunctionComponent<SourceEditorProps> = ({
    text = "",
    readonly = false,
    mode = "text"
}) => {

    const theme = getTheme();

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

    const editor = useConst(mode === "text" ? new Editor() : new XMLEditor());
    React.useEffect(() => {
        editor
            .text(text)
            .readOnly(readonly)
            .lazyRender()
            ;

        if (theme.semanticColors.link === darkTheme.palette.themePrimary) {
            editor.setOption("theme", "darcula");
        }

    }, [editor, readonly, text, theme.semanticColors.link]);

    const handleThemeToggle = (evt) => {
        if (!editor) return;
        if (evt.detail && evt.detail.dark === true) {
            editor.setOption("theme", "darcula");
        } else {
            editor.setOption("theme", "default");
        }
    };

    useOnEvent(document, "eclwatch-theme-toggle", handleThemeToggle);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} />}
        main={
            <AutosizeHpccJSComponent widget={editor} padding={4} />
        }
    />;
};

interface TextSourceEditorProps {
    text: string;
    readonly?: boolean;
}

export const TextSourceEditor: React.FunctionComponent<TextSourceEditorProps> = ({
    text = "",
    readonly = false
}) => {

    return <SourceEditor text={text} readonly={readonly} mode="text"></SourceEditor>;
};

interface XMLSourceEditorProps {
    text: string;
    readonly?: boolean;
}

export const XMLSourceEditor: React.FunctionComponent<XMLSourceEditorProps> = ({
    text = "",
    readonly = false
}) => {

    return <SourceEditor text={text} readonly={readonly} mode="xml"></SourceEditor>;
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

export interface WUResourceEditorProps {
    src: string;
}

export const WUResourceEditor: React.FunctionComponent<WUResourceEditorProps> = ({
    src
}) => {

    const [text, setText] = React.useState("");

    React.useEffect(() => {
        fetch(src).then(response => {
            return response.text();
        }).then(content => {
            setText(content);
        });
    }, [src]);

    return <SourceEditor text={text} readonly={true} mode="text"></SourceEditor>;
};

interface FetchEditor {
    url: string;
    readonly?: boolean;
    mode?: "ecl" | "xml" | "text";
}

export const FetchEditor: React.FunctionComponent<FetchEditor> = ({
    url,
    readonly = true,
    mode = "text"
}) => {

    const [text, setText] = React.useState("");

    React.useEffect(() => {
        fetch(url).then(response => {
            return response.text();
        }).then(content => {
            setText(content);
        });
    }, [url]);

    return <SourceEditor text={text} readonly={readonly} mode={mode}></SourceEditor>;
};

