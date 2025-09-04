import * as React from "react";
import { Toolbar, ToolbarButton, ToolbarDivider, ToolbarProps, ToolbarToggleButton } from "@fluentui/react-components";
import { CopyRegular, EyeRegular } from "@fluentui/react-icons";
import { useConst, useOnEvent } from "@fluentui/react-hooks";
import { Editor, CSSEditor, ECLEditor, XMLEditor, HTMLEditor, JSEditor, JSONEditor, SQLEditor, YAMLEditor, ICompletion } from "@hpcc-js/codemirror";
import { Workunit } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeHpccJSComponent } from "../layouts/HpccJSAdapter";
import { useUserTheme } from "../hooks/theme";
import { useWorkunitXML } from "../hooks/workunit";
import { IFrame } from "./IFrame";

import "hpcc/css/cmDarcula.css";

const logger = scopedLogger("src-react/components/SourceEditor.tsx");

export type ModeT = "css" | "ecl" | "html" | "js" | "json" | "sql" | "text" | "xml" | "yaml";

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
        case "css":
            return new CSSEditor();
        case "ecl":
            return new ECLEditor();
        case "html":
            return new HTMLEditor();
        case "js":
            return new JSEditor();
        case "json":
            return new JSONEditor();
        case "sql":
            return new SQLEditorEx();
        case "xml":
            return new XMLEditor();
        case "yaml":
            return new YAMLEditor();
        case "text":
        default:
            return new Editor();
    }
}

interface SourceEditorProps {
    mode?: ModeT;
    text?: string;
    previewUrl?: string;
    readonly?: boolean;
    toolbar?: boolean;
    onTextChange?: (text: string) => void;
    onFetchHints?: (cm: any, option: any) => Promise<ICompletion>;
    onSubmit?: () => void;
}

export const SourceEditor: React.FunctionComponent<SourceEditorProps> = ({
    mode = "text",
    text = "",
    previewUrl = "",
    readonly = false,
    toolbar = true,
    onTextChange = (text: string) => { },
    onFetchHints,
    onSubmit
}) => {

    const [checkedValues, setCheckedValues] = React.useState<Record<string, string[]>>({ displayOptions: ["preview"], });
    const onChange: ToolbarProps["onCheckedValueChange"] = (e, { name, checkedItems }) => {
        setCheckedValues((s) => s ? { ...s, [name]: checkedItems } : { [name]: checkedItems });
    };
    const { isDark } = useUserTheme();

    const editor = React.useMemo(() => newEditor(mode), [mode]);

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
        header={toolbar && (
            <Toolbar size="small" checkedValues={checkedValues} onCheckedValueChange={onChange}>
                <ToolbarButton icon={<CopyRegular />} onClick={() => navigator?.clipboard?.writeText(text)}>
                    {nlsHPCC.Copy}
                </ToolbarButton>
                <ToolbarDivider />
                <ToolbarToggleButton name="displayOptions" value="preview" icon={<EyeRegular />} disabled={mode !== "html"} appearance="transparent">
                    {nlsHPCC.Preview}
                </ToolbarToggleButton>
            </Toolbar>
        )}
        main={
            checkedValues?.displayOptions?.includes("preview") && mode === "html" ?
                <IFrame src={previewUrl} padding={"4px 0"} /> :
                <AutosizeHpccJSComponent widget={editor} padding={4} />
        }
    />;
};

interface TextSourceEditorProps {
    text: string;
    readonly?: boolean;
    toolbar?: boolean;
}

export const TextSourceEditor: React.FunctionComponent<TextSourceEditorProps> = ({
    text = "",
    readonly,
    toolbar
}) => {

    return <SourceEditor text={text} toolbar={toolbar} readonly={readonly} mode="text"></SourceEditor>;
};

interface XMLSourceEditorProps {
    text: string;
    readonly?: boolean;
    toolbar?: boolean;
}

export const XMLSourceEditor: React.FunctionComponent<XMLSourceEditorProps> = ({
    text = "",
    readonly,
    toolbar
}) => {

    return <SourceEditor text={text} toolbar={toolbar} readonly={readonly} mode="xml"></SourceEditor>;
};

interface JSONSourceEditorProps {
    json?: object;
    readonly?: boolean;
    toolbar?: boolean;
    onChange?: (obj: object) => void;
}

export const JSONSourceEditor: React.FunctionComponent<JSONSourceEditorProps> = ({
    json,
    readonly,
    toolbar,
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

    return <SourceEditor text={text} toolbar={toolbar} readonly={readonly} mode="json" onTextChange={textChanged}></SourceEditor>;
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
    toolbar?: boolean;
}

export const WUResourceEditor: React.FunctionComponent<WUResourceEditorProps> = ({
    src,
    toolbar
}) => {

    const [text, setText] = React.useState("");

    React.useEffect(() => {
        fetch(src).then(response => {
            return response.text();
        }).then(content => {
            setText(content);
        });
    }, [src]);

    return <SourceEditor text={text} toolbar={toolbar} readonly={true} mode="text"></SourceEditor>;
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
    toolbar?: boolean;
    noDataMsg?: string;
    loadingMsg?: string;
    mode?: ModeT;
}

export const FetchEditor: React.FunctionComponent<FetchEditor> = ({
    url,
    wuid,
    readonly = true,
    toolbar,
    noDataMsg = nlsHPCC.noDataMessage,
    loadingMsg = nlsHPCC.loadingMessage,
    mode = "text"
}) => {

    const [text, setText] = React.useState("");
    const [exceptionUrl, setExceptionUrl] = React.useState("");

    React.useEffect(() => {
        setText(loadingMsg);
        let cancelled = false;
        if (wuid) {
            const wu = Workunit.attach({ baseUrl: "" }, wuid);
            wu.fetchQuery().then(function (query) {
                if (!cancelled) {
                    setText(query?.Text ?? "");
                }
            }).catch(e => {
                logger.error(e);
                if (!cancelled) {
                    setText(e.message);
                }
            });
        } else if (url) {
            const controller = new AbortController();
            fetch(url, { signal: controller.signal }).then(response => {
                if (!cancelled) {
                    const contentType = response.headers.get("Content-Type") || "";
                    return response.text().then(content => ({ contentType, content }));
                }
            }).then(({ contentType, content }) => {
                if (!cancelled) {
                    if (contentType.includes("text/html") && (content.includes("<html>") || content.includes("<!DOCTYPE html>")) && content.includes("Exception")) {
                        setText("");
                        setExceptionUrl(url);
                    } else {
                        setText(content);
                        setExceptionUrl("");
                    }
                }
            }).catch(e => {
                logger.error(e);
                if (!cancelled) {
                    setText(e.message);
                }
            });
        } else {
            setText(noDataMsg);
        }
        return () => {
            cancelled = true;
        };
    }, [loadingMsg, noDataMsg, url, wuid]);

    return exceptionUrl ?
        <IFrame src={exceptionUrl} padding={"4px 0"} /> :
        <SourceEditor text={text} previewUrl={url} toolbar={toolbar} readonly={readonly} mode={mode}></SourceEditor>;
};

interface SQLSourceEditorProps {
    sql: string;
    readonly?: boolean;
    toolbar?: boolean;
    onSqlChange?: (sql: string) => void;
    onFetchHints?: (cm: any, option: any) => Promise<ICompletion>;
    onSubmit?: () => void;
}

export const SQLSourceEditor: React.FunctionComponent<SQLSourceEditorProps> = ({
    sql,
    readonly,
    toolbar,
    onSqlChange,
    onFetchHints,
    onSubmit
}) => {
    return <SourceEditor text={sql} toolbar={toolbar} readonly={readonly} onTextChange={onSqlChange} onFetchHints={onFetchHints} onSubmit={onSubmit} mode={"sql"}></SourceEditor>;
};

