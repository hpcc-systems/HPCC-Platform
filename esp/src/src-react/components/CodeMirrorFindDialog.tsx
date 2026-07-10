import * as React from "react";
import { Button, Checkbox, Field, Input, makeStyles } from "@fluentui/react-components";
import { Editor } from "@hpcc-js/codemirror";
import nlsHPCC from "src/nlsHPCC";
import { MessageBox } from "../layouts/MessageBox";

const useStyles = makeStyles({
    findDialogContent: {
        display: "flex",
        gap: "16px",
        alignItems: "flex-start",
    },
    findDialogMainColumn: {
        display: "flex",
        flexDirection: "column",
        gap: "10px",
        minWidth: "260px",
    },
    findDialogButtons: {
        display: "flex",
        flexDirection: "column",
        gap: "8px",
        minWidth: "110px",
    },
    findDialogOptions: {
        display: "flex",
        flexDirection: "column",
        gap: "6px",
    },
    findMatchHighlight: {
        backgroundColor: "#fff59d",
        color: "#000000",
    }
});

const WORD_CHAR_REGEX = /[A-Za-z0-9_]/;

function isWholeWordMatch(text: string, index: number, length: number): boolean {
    const before = index > 0 ? text[index - 1] : "";
    const after = index + length < text.length ? text[index + length] : "";
    return !WORD_CHAR_REGEX.test(before) && !WORD_CHAR_REGEX.test(after);
}

function findNextIndex(text: string, query: string, startIndex: number, matchCase: boolean, wholeWord: boolean, endIndex = text.length): number {
    const source = matchCase ? text : text.toLowerCase();
    const needle = matchCase ? query : query.toLowerCase();
    let cursor = Math.max(0, startIndex);

    while (cursor <= source.length - needle.length) {
        const foundIndex = source.indexOf(needle, cursor);
        if (foundIndex < 0 || foundIndex >= endIndex) {
            return -1;
        }
        if (!wholeWord || isWholeWordMatch(text, foundIndex, query.length)) {
            return foundIndex;
        }
        cursor = foundIndex + 1;
    }

    return -1;
}

interface LastFind {
    query: string;
    matchCase: boolean;
    matchWholeWord: boolean;
    index: number;
    length: number;
}

type CodeMirrorKeyHandler = (cm?: unknown) => boolean | void;
type CodeMirrorExtraKeys = Record<string, string | CodeMirrorKeyHandler>;

type EditorWithUnknownOptions = Omit<Editor, "option"> & {
    option(option: string): unknown;
    option(option: string, value: unknown): Editor;
};

export interface CodeMirrorFindDialogProps {
    editor: Editor;
    hotkeys?: boolean;
    title?: string;
    minWidth?: number;
    openRequestKey?: number;
}

export const CodeMirrorFindDialog: React.FunctionComponent<CodeMirrorFindDialogProps> = ({
    editor,
    hotkeys = true,
    title = nlsHPCC.Find,
    minWidth = 420,
    openRequestKey
}) => {
    const [show, setShow] = React.useState(false);
    const [findText, setFindText] = React.useState("");
    const [matchCase, setMatchCase] = React.useState(false);
    const [matchWholeWord, setMatchWholeWord] = React.useState(false);
    const [lastFind, setLastFind] = React.useState<LastFind>();
    const findInputRef = React.useRef<HTMLInputElement>(null);
    const openRequestKeyInitialized = React.useRef(false);
    const prevOpenRequestKey = React.useRef<number | undefined>(undefined);
    const styles = useStyles();

    const clearFindHighlight = React.useCallback(() => {
        editor.removeAllHighlight().lazyRender();
    }, [editor]);

    const openDialog = React.useCallback(() => {
        setShow(true);
        return true;
    }, []);

    const handleFindNext = React.useCallback(() => {
        const query = findText;
        if (!query) {
            return;
        }

        const text = editor.text();
        if (!text) {
            return;
        }

        const continueFromCurrent = lastFind?.query === query && lastFind?.matchCase === matchCase && lastFind?.matchWholeWord === matchWholeWord;
        const startIndex = continueFromCurrent ? lastFind.index + lastFind.length : 0;

        let foundIndex = findNextIndex(text, query, startIndex, matchCase, matchWholeWord);
        if (foundIndex < 0 && startIndex > 0) {
            foundIndex = findNextIndex(text, query, 0, matchCase, matchWholeWord, startIndex);
        }

        if (foundIndex < 0) {
            clearFindHighlight();
            setLastFind(undefined);
            return;
        }

        const pos = editor.positionAt(foundIndex);
        editor.removeAllHighlight();
        editor.highlight(foundIndex, foundIndex + query.length, styles.findMatchHighlight);
        editor.setCursor(pos.line, pos.ch, true);
        editor.lazyRender();

        setLastFind({
            query,
            matchCase,
            matchWholeWord,
            index: foundIndex,
            length: query.length
        });
    }, [clearFindHighlight, editor, findText, lastFind, matchCase, matchWholeWord, styles.findMatchHighlight]);

    const closeDialog = React.useCallback(() => {
        setShow(false);
        setLastFind(undefined);
        clearFindHighlight();
    }, [clearFindHighlight]);

    React.useEffect(() => {
        setLastFind(undefined);
        editor.removeAllHighlight();
    }, [editor, findText, matchCase, matchWholeWord]);

    React.useEffect(() => {
        if (!hotkeys) {
            return;
        }
        const onKeyDown = (evt: KeyboardEvent) => {
            const editorHasFocus = (editor as { hasFocus?: () => boolean })?.hasFocus?.() ?? false;
            if ((evt.ctrlKey || evt.metaKey) && evt.key.toLowerCase() === "f" && editorHasFocus) {
                evt.preventDefault();
                evt.stopPropagation();
                openDialog();
            }
        };

        document.addEventListener("keydown", onKeyDown, true);
        return () => {
            document.removeEventListener("keydown", onKeyDown, true);
        };
    }, [editor, hotkeys, openDialog]);

    React.useEffect(() => {
        if (!hotkeys) {
            return;
        }

        const editorWithOptions = editor as EditorWithUnknownOptions;
        const previousExtraKeys = editorWithOptions.option("extraKeys") as CodeMirrorExtraKeys | undefined;
        const findNext: CodeMirrorKeyHandler = () => {
            handleFindNext();
            return true;
        };

        editorWithOptions.option("extraKeys", {
            ...previousExtraKeys,
            "Ctrl-F": openDialog,
            "Cmd-F": openDialog,
            F3: findNext,
        });

        return () => {
            editorWithOptions.option("extraKeys", previousExtraKeys ?? null);
        };
    }, [editor, handleFindNext, hotkeys, openDialog]);

    React.useEffect(() => {
        if (!hotkeys || !show) {
            return;
        }
        const onKeyDown = (evt: KeyboardEvent) => {
            if (evt.key === "F3") {
                evt.preventDefault();
                handleFindNext();
            }
        };

        document.addEventListener("keydown", onKeyDown);
        return () => {
            document.removeEventListener("keydown", onKeyDown);
        };
    }, [handleFindNext, hotkeys, show]);

    React.useEffect(() => {
        if (!show) {
            return;
        }
        const t = window.setTimeout(() => {
            findInputRef.current?.focus();
            findInputRef.current?.select();
        }, 0);
        return () => {
            window.clearTimeout(t);
        };
    }, [show]);

    React.useEffect(() => {
        if (!openRequestKeyInitialized.current) {
            prevOpenRequestKey.current = openRequestKey;
            openRequestKeyInitialized.current = true;
            return;
        }

        if (openRequestKey !== undefined && prevOpenRequestKey.current !== openRequestKey) {
            setShow(true);
        }
        prevOpenRequestKey.current = openRequestKey;
    }, [openRequestKey]);

    React.useEffect(() => {
        return () => {
            editor.removeAllHighlight().lazyRender();
        };
    }, [editor]);

    return <MessageBox title={title} minWidth={minWidth} show={show} onDismiss={closeDialog} setShow={setShow}>
        <div
            className={styles.findDialogContent}
            onKeyDown={evt => {
                if (evt.key === "Escape") {
                    evt.preventDefault();
                    closeDialog();
                    return;
                }
                if (evt.key === "Enter" && (evt.target as HTMLElement | null)?.tagName === "INPUT") {
                    evt.preventDefault();
                    handleFindNext();
                }
            }}
        >
            <div className={styles.findDialogMainColumn}>
                <Field label={`${nlsHPCC.Find}:`}>
                    <Input
                        ref={findInputRef}
                        value={findText}
                        onChange={(_, data) => setFindText(data.value)}
                    />
                </Field>
                <div className={styles.findDialogOptions}>
                    <Checkbox
                        label={nlsHPCC.MatchCase}
                        checked={matchCase}
                        onChange={(_, data) => setMatchCase(!!data.checked)}
                    />
                    <Checkbox
                        label={nlsHPCC.MatchWholeWord}
                        checked={matchWholeWord}
                        onChange={(_, data) => setMatchWholeWord(!!data.checked)}
                    />
                </div>
            </div>
            <div className={styles.findDialogButtons}>
                <Button appearance="primary" onClick={handleFindNext}>{nlsHPCC.FindNext}</Button>
                <Button onClick={closeDialog}>{nlsHPCC.Cancel}</Button>
            </div>
        </div>
    </MessageBox>;
};
