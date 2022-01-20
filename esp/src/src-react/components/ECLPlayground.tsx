import * as React from "react";
import { ReflexContainer, ReflexElement, ReflexSplitter } from "../layouts/react-reflex";
import { getTheme, PrimaryButton, IconButton, IIconProps, Link, Dropdown, IDropdownOption } from "@fluentui/react";
import { useOnEvent } from "@fluentui/react-hooks";
import { mergeStyleSets } from "@fluentui/style-utilities";
import { ECLEditor, IPosition } from "@hpcc-js/codemirror";
import { Workunit } from "@hpcc-js/comms";
import { HolyGrail } from "../layouts/HolyGrail";
import { DojoAdapter } from "../layouts/DojoAdapter";
import { pushUrl } from "../util/history";
import { darkTheme } from "../themes";
import { InfoGrid } from "./InfoGrid";
import { TabbedResults } from "./Results";
import { ECLSourceEditor } from "./SourceEditor";
import { TargetClusterTextField } from "./forms/Fields";
import nlsHPCC from "src/nlsHPCC";

import "eclwatch/css/cmDarcula.css";

interface ECLPlaygroundProps {
    wuid?: string;
}

const enum OutputMode {
    ERRORS = "errors",
    RESULTS = "results",
    VIS = "vis"
}

const borderStyle = "1px solid darkgrey";

const playgroundStyles = mergeStyleSets({
    root: {
        height: "100%",
        border: borderStyle,
        selectors: {
            ".reflex-container > .reflex-splitter": {
                backgroundColor: "transparent",
                display: "flex",
                alignItems: "center",
                justifyContent: "space-evenly",
                ":hover": {
                    backgroundColor: "transparent",
                }
            },
            ".reflex-container.horizontal > .reflex-splitter": {
                height: "2px",
                padding: "1px",
                borderTop: borderStyle,
                borderBottom: borderStyle,
                "::after": {
                    content: "' '",
                    borderBottom: "2px solid #9e9e9e",
                    width: "19px",
                    marginLeft: "-19px"
                },
                ":hover": {
                    borderTop: borderStyle,
                    borderBottom: borderStyle
                }
            },
            ".reflex-container.vertical > .reflex-splitter": {
                width: "2px",
                padding: "1px",
                borderLeft: borderStyle,
                borderRight: borderStyle,
                "::after": {
                    content: "' '",
                    borderLeft: "2px solid #9e9e9e",
                    height: "19px",
                },
                ":hover": {
                    borderLeft: borderStyle,
                    borderRight: borderStyle
                }
            }
        },
    },
    titleBar: {
        padding: "4px 8px",
        height: "32px",
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        borderBottom: borderStyle
    },
    title: {
        fontSize: "24px",
        lineHeight: "24px",
        fontWeight: "bold",
        display: "inline-block",
        margin: 0,
    },
    toolBar: {
        display: "flex",
        flexDirection: "row",
        padding: "4px 8px 8px 8px"
    },
    controlsWrapper: {
        display: "flex",
        flexGrow: 1
    },
    inlineDropdown: {
        display: "flex",
        marginLeft: "18px",
        "label": {
            marginRight: "8px"
        },
        ".ms-Dropdown": {
            minWidth: "100px"
        }
    },
    samplesDropdown: {
        ".ms-Dropdown": {
            width: "240px"
        }
    },
    outputButtons: {
        marginLeft: "18px"
    },
    statusMessage: {
        alignSelf: "center",
        minWidth: "100px"
    },
    fullscreen: {
        position: "absolute" as const,
        top: 0,
        left: 0,
        width: "100%",
        height: "100%",
        backgroundColor: "#fff"
    },
});

const warningIcon: IIconProps = { title: nlsHPCC.ErrorWarnings, ariaLabel: nlsHPCC.ErrorWarnings, iconName: "Warning" };
const resultsIcon: IIconProps = { title: nlsHPCC.Outputs, ariaLabel: nlsHPCC.Outputs, iconName: "Table" };
const graphIcon: IIconProps = { title: nlsHPCC.Visualizations, ariaLabel: nlsHPCC.Visualizations, iconName: "BarChartVerticalFill" };

const displayErrors = (wu, editor) => {
    if (!editor) return;
    wu.fetchECLExceptions().then(errors => {
        errors.forEach(err => {
            const lineError = err.LineNo;
            const lineErrorNum = lineError > 0 ? lineError - 1 : 0;
            const startPos: IPosition = {
                ch: (err.Column > 0) ? err.Column - 1 : 0,
                line: lineErrorNum
            };
            const endPos: IPosition = {
                ch: editor.getLineLength(lineErrorNum),
                line: lineErrorNum
            };

            switch (err.Severity) {
                case "Info":
                    editor.highlightInfo(startPos, endPos);
                    break;
                case "Warning":
                    editor.highlightWarning(startPos, endPos);
                    break;
                case "Error":
                default:
                    editor.highlightError(startPos, endPos);
                    break;
            }
        });
    });
};

interface ECLEditorToolbarProps {
    editor: ECLEditor;
    outputMode: OutputMode;
    setOutputMode: (_: OutputMode) => void;
    workunit: Workunit;
    setWorkunit: (_: Workunit) => void;
}

const ECLEditorToolbar: React.FunctionComponent<ECLEditorToolbarProps> = ({
    editor,
    outputMode,
    setOutputMode,
    workunit,
    setWorkunit
}) => {

    const [cluster, setCluster] = React.useState("");
    const [wuState, setWuState] = React.useState("");

    const submitWU = React.useCallback(async () => {
        const wu = await Workunit.create({ baseUrl: "" });

        await wu.update({ QueryText: editor.ecl() });
        await wu.submit(cluster);

        wu.watchUntilComplete(changes => {
            setWuState(wu.State);
            if (wu.isFailed()) {
                pushUrl(`/play/${wu.Wuid}`);
                setWorkunit(wu);
                displayErrors(wu, editor);
                setOutputMode(OutputMode.ERRORS);
            } else if (wu.isComplete()) {
                pushUrl(`/play/${wu.Wuid}`);
                setWorkunit(wu);
                setOutputMode(OutputMode.RESULTS);
            }
        });
    }, [cluster, editor, setOutputMode, setWorkunit]);

    const handleKeyUp = React.useCallback((evt) => {
        switch (evt.key) {
            case "Enter":
                if (evt.ctrlKey) {
                    submitWU();
                }
                break;
        }
    }, [submitWU]);
    useOnEvent(window, "keyup", handleKeyUp);

    React.useEffect(() => {
        if (!workunit) return;
        if (workunit.State) {
            setWuState(workunit.State);
        }
        if (workunit.Cluster) {
            setCluster(workunit.Cluster);
        }
    }, [workunit]);

    return <div className={playgroundStyles.toolBar}>
        <div className={playgroundStyles.controlsWrapper}>
            <PrimaryButton text={nlsHPCC.Submit} onClick={submitWU} />
            <TargetClusterTextField
                key="target-cluster"
                label={nlsHPCC.Target}
                className={playgroundStyles.inlineDropdown}
                onChange={React.useCallback((evt, option) => setCluster(option.key.toString()), [setCluster])}
                required={true}
                selectedKey={cluster ? cluster : undefined}
                options={[]}
            />
            <div className={playgroundStyles.outputButtons}>
                <IconButton
                    iconProps={warningIcon}
                    onClick={React.useCallback(() => setOutputMode(OutputMode.ERRORS), [setOutputMode])}
                    checked={outputMode === OutputMode.ERRORS ? true : false}
                />
                <IconButton
                    iconProps={resultsIcon}
                    onClick={React.useCallback(() => setOutputMode(OutputMode.RESULTS), [setOutputMode])}
                    checked={outputMode === OutputMode.RESULTS ? true : false}
                    disabled={workunit?.Wuid ? false : true}
                />
                <IconButton
                    iconProps={graphIcon}
                    onClick={React.useCallback(() => setOutputMode(OutputMode.VIS), [setOutputMode])}
                    checked={outputMode === OutputMode.VIS ? true : false}
                    disabled={workunit?.Wuid ? false : true}
                />
            </div>
        </div>
        <div className={playgroundStyles.statusMessage}>
            <Link href={(workunit?.Wuid) ? `#/workunits/${workunit.Wuid}` : ""}>{wuState}</Link>
        </div>
    </div>;
};

export const ECLPlayground: React.FunctionComponent<ECLPlaygroundProps> = (props) => {

    const { wuid } = props;
    const theme = getTheme();

    const [outputMode, setOutputMode] = React.useState<OutputMode>(OutputMode.ERRORS);
    const [workunit, setWorkunit] = React.useState<Workunit>();
    const [editor, setEditor] = React.useState<ECLEditor>();
    const [query, setQuery] = React.useState("");
    const [eclSamples, setEclSamples] = React.useState<IDropdownOption[]>([]);

    React.useEffect(() => {
        if (wuid) {
            const wu = Workunit.attach({ baseUrl: "" }, wuid);
            wu.fetchQuery().then(result => {
                setWorkunit(wu);
                setQuery(result.Text);
                if (wu.isFailed()) {
                    displayErrors(wu, editor);
                    setOutputMode(OutputMode.ERRORS);
                } else if (wu.isComplete()) {
                    setOutputMode(OutputMode.RESULTS);
                }
            });
        }

        fetch("/esp/files/eclwatch/ecl/ECLPlaygroundSamples.json")
            .then(response => response.json())
            .then(json => setEclSamples(
                json.items.map(item => {
                    return { key: item.filename, text: item.name };
                })
            ));

        if (editor) {
            if (theme.semanticColors.link === darkTheme.palette.themePrimary) {
                editor.setOption("theme", "darcula");
            }
        }
    }, [wuid, editor, theme]);

    const handleThemeToggle = React.useCallback((evt) => {
        if (!editor) return;
        if (evt.detail && evt.detail.dark === true) {
            editor.setOption("theme", "darcula");
        } else {
            editor.setOption("theme", "default");
        }
    }, [editor]);
    useOnEvent(document, "eclwatch-theme-toggle", handleThemeToggle);

    return <div className={playgroundStyles.root}>
        <div className={playgroundStyles.titleBar}>
            <h1 className={playgroundStyles.title}>{nlsHPCC.title_ECLPlayground}</h1>
            <Dropdown
                label="Sample"
                className={`${playgroundStyles.inlineDropdown} ${playgroundStyles.samplesDropdown}`}
                options={eclSamples}
                placeholder="Select sample ECL..."
                onChange={(evt, item) => {
                    fetch(`/esp/files/eclwatch/ecl/${item.key}`)
                        .then(async response => {
                            if (response.status && response.status === 200 && response.body) {
                                const eclSample = await response.text();
                                editor.ecl(eclSample);
                            }
                        });
                }}
            />
        </div>
        <ReflexContainer orientation="horizontal">
            <ReflexElement>
                <ReflexContainer orientation="vertical">
                    <ReflexElement>
                        <HolyGrail
                            main={<ECLSourceEditor text={query} setEditor={setEditor} />}
                            footer={
                                <ECLEditorToolbar
                                    editor={editor}
                                    workunit={workunit} setWorkunit={setWorkunit}
                                    outputMode={outputMode} setOutputMode={setOutputMode}
                                />
                            }
                        />
                    </ReflexElement>
                    <ReflexSplitter />
                    <ReflexElement minSize={100} flex={0.25} style={{ overflow: "hidden" }}>
                        <DojoAdapter widgetClassID="Graph7Widget" params={{ Wuid: workunit?.Wuid }} />
                    </ReflexElement>
                </ReflexContainer>
            </ReflexElement>
            <ReflexSplitter />
            <ReflexElement propagateDimensions={true} minSize={100}>
                {outputMode === OutputMode.ERRORS ? (
                    <InfoGrid wuid={workunit?.Wuid} />

                ) : outputMode === OutputMode.RESULTS ? (
                    <TabbedResults wuid={workunit?.Wuid} />

                ) : outputMode === OutputMode.VIS ? (
                    <div style={{ height: "calc(100% - 25px)" }}>
                        <DojoAdapter widgetClassID="VizWidget" params={{ Wuid: workunit?.Wuid, Sequence: 0 }} />
                    </div>
                ) : null}
            </ReflexElement>
        </ReflexContainer>
    </div>;
};