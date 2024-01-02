import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, useTheme } from "@fluentui/react";
import { useConst, useOnEvent } from "@fluentui/react-hooks";
import { Editor, ECLEditor, XMLEditor, JSONEditor } from "@hpcc-js/codemirror";
import { Workunit } from "@hpcc-js/comms";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { useWorkunitXML } from "../hooks/workunit";
import { themeIsDark } from "src/Utility";
import { ShortVerticalDivider } from "./Common";
import "eclwatch/css/cmDarcula.css";

type ModeT = "ecl" | "xml" | "json" | "text";

function newEditor(mode: ModeT) {
    switch (mode) {
        case "ecl":
            return new ECLEditor();
        case "xml":
            return new XMLEditor();
        case "json":
            return new JSONEditor();
        case "text":
        default:
            return new Editor();
    }
}

interface SourceEditorProps {
    mode?: ModeT;
    text?: string;
    readonly?: boolean;
    onChange?: (text: string) => void;
}

export const SourceEditor: React.FunctionComponent<SourceEditorProps> = ({
    mode = "text",
    text = "",
    readonly = false,
    onChange = (text: string) => { }
}) => {

    const theme = useTheme();

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

    const editor = useConst(() => newEditor(mode)
        .on("changes", () => {
            onChange(editor.text());
        })
    );

    React.useEffect(() => {
        try {
            const t = window.setTimeout(function () {
                if (themeIsDark()) {
                    editor.setOption("theme", "darcula");
                } else {
                    editor.setOption("theme", "default");
                }
                window.clearTimeout(t);
            }, 50);
        } catch (e) { } // editor's internal codemirror is possibly undefined?

        if (editor.text() !== text) {
            editor.text(text);
        }

        editor
            .readOnly(readonly)
            .lazyRender()
            ;
    }, [editor, readonly, text, theme]);

    const handleThemeToggle = React.useCallback((evt) => {
        if (!editor) return;
        if (evt.detail && evt.detail.dark === true) {
            editor.setOption("theme", "darcula");
        } else {
            editor.setOption("theme", "default");
        }
    }, [editor]);
    useOnEvent(document, "eclwatch-theme-toggle", handleThemeToggle);

    return <HolyGrail
        header={<CommandBar items={buttons} />}
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

interface JSONSourceEditorProps {
    json?: object;
    readonly?: boolean;
    onChange?: (obj: object) => void;
}

export const JSONSourceEditor: React.FunctionComponent<JSONSourceEditorProps> = ({
    json,
    readonly = false,
    onChange = (obj: object) => { }
}) => {

    const text = React.useMemo(() => {
        try {
            return JSON.stringify(json, undefined, 4);
        } catch (e) {
            return "";
        }
    }, [json]);

    const textChanged = React.useCallback((text) => {
        try {
            onChange(JSON.parse(text));
        } catch (e) {
        }
    }, [onChange]);

    return <SourceEditor text={text} readonly={readonly} mode="json" onChange={textChanged}></SourceEditor>;
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

interface ECLSourceEditorProps {
    text: string,
    readonly?: boolean;
    setEditor?: (_: any) => void;
}

export const ECLSourceEditor: React.FunctionComponent<ECLSourceEditorProps> = ({
    text = "",
    readonly = false,
    setEditor
}) => {

    const editor = useConst(() => new ECLEditor());
    React.useEffect(() => {
        editor
            .text(text)
            .readOnly(readonly)
            .lazyRender()
            ;

        if (setEditor) {
            setEditor(editor);
        }
    }, [editor, readonly, text, setEditor]);

    return <AutosizeHpccJSComponent widget={editor} padding={4} />;
};

interface FetchEditor {
    url: string;
    wuid?: string;
    readonly?: boolean;
    mode?: "ecl" | "xml" | "text";
}

export const FetchEditor: React.FunctionComponent<FetchEditor> = ({
    url,
    wuid,
    readonly = true,
    mode = "text"
}) => {

    const [text, setText] = React.useState("");

    React.useEffect(() => {
        if (wuid) {
            const wu = Workunit.attach({ baseUrl: "" }, wuid);
            wu.fetchQuery().then(function (query) {
                setText(query?.Text ?? "");
            });
        } else {
            fetch(url).then(response => {
                return response.text();
            }).then(content => {
                setText(content);
            });
        }
    }, [url, wuid]);

    return <SourceEditor text={text} readonly={readonly} mode={mode}></SourceEditor>;
};

