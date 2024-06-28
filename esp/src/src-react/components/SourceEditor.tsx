import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst, useOnEvent } from "@fluentui/react-hooks";
import { Editor, ECLEditor, XMLEditor, JSONEditor, SQLEditor, ICompletion } from "@hpcc-js/codemirror";
import { Workunit } from "@hpcc-js/comms";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { useUserTheme } from "../hooks/theme";
import { useWorkunitXML } from "../hooks/workunit";
import { ShortVerticalDivider } from "./Common";

import "eclwatch/css/cmDarcula.css";

type ModeT = "ecl" | "xml" | "json" | "text" | "sql";

class SQLEditorEx extends SQLEditor {

    constructor() {
        super();
    }

    enter(domNode, element) {
        super.enter(domNode, element);
        this.option("extraKeys", {
            "Ctrl-Enter": cm => {
                this.submit();
            },
            "Ctrl-S": cm => {
                this.submit();
            }

        } as any);
    }

    submit() {
    }
}

function newEditor(mode: ModeT) {
    switch (mode) {
        case "ecl":
            return new ECLEditor();
        case "xml":
            return new XMLEditor();
        case "json":
            return new JSONEditor();
        case "sql":
            return new SQLEditorEx();
        case "text":
        default:
            return new Editor();
    }
}

interface SourceEditorProps {
    mode?: ModeT;
    text?: string;
    readonly?: boolean;
    toolbar?: boolean;
    onTextChange?: (text: string) => void;
    onFetchHints?: (cm: any, option: any) => Promise<ICompletion>;
    onSubmit?: () => void;
}

export const SourceEditor: React.FunctionComponent<SourceEditorProps> = ({
    mode = "text",
    text = "",
    readonly = false,
    toolbar = true,
    onTextChange = (text: string) => { },
    onFetchHints,
    onSubmit
}) => {

    const { isDark } = useUserTheme();

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

    const editor = useConst(() => newEditor(mode));

    React.useEffect(() => {
        editor
            .on("changes", onTextChange ? () => onTextChange(editor.text()) : undefined, true)
            ;
    }, [editor, onTextChange]);

    React.useEffect(() => {
        editor
            .showHints(onFetchHints !== undefined)
            .on("fetchHints", (cm, option) => {
                if (onFetchHints) {
                    return onFetchHints(cm, option);
                }
                return Promise.resolve(null);
            }, true)
            ;
    }, [editor, onFetchHints]);

    React.useEffect(() => {
        if (onSubmit) {
            editor
                .on("submit", onSubmit ? () => onSubmit() : undefined, true)
                ;
        }
    }, [editor, onSubmit]);

    const handleThemeToggle = React.useCallback((evt) => {
        if (!editor) return;
        if (evt.detail && evt.detail.dark === true) {
            editor.option("theme", "darcula");
        } else {
            editor.option("theme", "default");
        }
    }, [editor]);
    useOnEvent(document, "eclwatch-theme-toggle", handleThemeToggle);

    React.useEffect(() => {
        editor.option("theme", isDark ? "darcula" : "default");
        if (editor.text() !== text) {
            editor.text(text);
        }

        editor
            .readOnly(readonly)
            .lazyRender()
            ;
    }, [editor, text, readonly, isDark]);

    return <HolyGrail
        header={toolbar && <CommandBar items={buttons} />}
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

    return <SourceEditor text={text} readonly={readonly} mode="json" onTextChange={textChanged}></SourceEditor>;
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

interface SQLSourceEditorProps {
    sql: string;
    toolbar?: boolean;
    onSqlChange?: (sql: string) => void;
    onFetchHints?: (cm: any, option: any) => Promise<ICompletion>;
    onSubmit?: () => void;
}

export const SQLSourceEditor: React.FunctionComponent<SQLSourceEditorProps> = ({
    sql,
    toolbar,
    onSqlChange,
    onFetchHints,
    onSubmit
}) => {
    return <SourceEditor text={sql} toolbar={toolbar} onTextChange={onSqlChange} onFetchHints={onFetchHints} onSubmit={onSubmit} mode={"sql"}></SourceEditor>;
};

