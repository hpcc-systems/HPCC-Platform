import * as React from "react";
import { ReflexContainer, ReflexElement, ReflexSplitter } from "../layouts/react-reflex";
import { IconButton, IIconProps, Link, Dropdown, IDropdownOption, TextField, useTheme } from "@fluentui/react";
import { Button } from "@fluentui/react-components";
import { CheckmarkCircleRegular, DismissCircleRegular, QuestionCircleRegular } from "@fluentui/react-icons";
import { scopedLogger } from "@hpcc-js/util";
import { useOnEvent } from "@fluentui/react-hooks";
import { mergeStyleSets } from "@fluentui/style-utilities";
import { ECLEditor, IPosition } from "@hpcc-js/codemirror";
import { Workunit, WUUpdate, WorkunitsService } from "@hpcc-js/comms";
import { HolyGrail } from "../layouts/HolyGrail";
import { DojoAdapter } from "../layouts/DojoAdapter";
import { pushUrl } from "../util/history";
import { debounce } from "../util/throttle";
import { darkTheme } from "../themes";
import { InfoGrid } from "./InfoGrid";
import { TabbedResults } from "./Results";
import { ECLSourceEditor } from "./SourceEditor";
import { TargetClusterOption, TargetClusterTextField } from "./forms/Fields";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("../components/ECLPlayground.tsx");

interface ECLPlaygroundProps {
    wuid?: string;
    ecl?: string;
    filter?: { [id: string]: any };
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
            },
            ".fui-Button": {
                height: "min-content"
            },
            ".ms-Label": {
                marginRight: "12px"
            },
            ".ms-Label::after": {
                paddingRight: 0
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
    publishWrapper: {
        display: "flex",
        ".ms-TextField-wrapper": {
            display: "flex",
            marginLeft: "18px"
        },
        ".ms-TextField-errorMessage": {
            display: "none"
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

const displayErrors = async (wu = null, editor, errors = []) => {
    if (!editor) return;
    if (wu) {
        errors = await wu.fetchECLExceptions();
    }
    if (!errors.length) {
        editor.removeAllHighlight();
    }
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
};

const service = new WorkunitsService({ baseUrl: "" });

enum SyntaxCheckResult {
    Unknown,
    Failed,
    Passed
}

interface ECLEditorToolbarProps {
    editor: ECLEditor;
    outputMode: OutputMode;
    setOutputMode: (_: OutputMode) => void;
    workunit: Workunit;
    setWorkunit: (_: Workunit) => void;
    setSyntaxErrors: (_: any) => void;
    syntaxStatusIcon: number;
    setSyntaxStatusIcon: (_: number) => void;
}

const ECLEditorToolbar: React.FunctionComponent<ECLEditorToolbarProps> = ({
    editor,
    outputMode,
    setOutputMode,
    workunit,
    setWorkunit,
    setSyntaxErrors,
    syntaxStatusIcon,
    setSyntaxStatusIcon
}) => {

    const [cluster, setCluster] = React.useState("");
    const [wuState, setWuState] = React.useState("");
    const [queryName, setQueryName] = React.useState("");
    const queryNameRef = React.useRef(null);
    const [queryNameErrorMsg, setQueryNameErrorMsg] = React.useState("");
    const [showSubmitBtn, setShowSubmitBtn] = React.useState(true);

    const playgroundResults = React.useCallback((wu, action = "submit") => {
        setWuState(wu.State);
        if (document.location.hash.includes("play")) {
            if (wu.isFailed()) {
                pushUrl(`/play/${wu.Wuid}`);
                setWorkunit(wu);
                displayErrors(wu, editor);
                setOutputMode(OutputMode.ERRORS);
            } else if (wu.isComplete()) {
                pushUrl(`/play/${wu.Wuid}`);
                if (action === "publish") {
                    wu.publish(queryName);
                    setWuState("Published");
                }
                setWorkunit(wu);
                setOutputMode(OutputMode.RESULTS);
            }
        } else {
            if (wu.isComplete()) {
                logger.info(`${nlsHPCC.Playground} ${nlsHPCC.Finished} (${wu.Wuid})`);
            }
        }
    }, [editor, queryName, setOutputMode, setWorkunit]);

    const submitWU = React.useCallback(async () => {
        const wu = await Workunit.create({ baseUrl: "" });

        await wu.update({ Jobname: queryName, QueryText: editor.ecl() });
        await wu.submit(cluster);

        wu.watchUntilComplete(changes => playgroundResults(wu));
    }, [cluster, editor, playgroundResults, queryName]);

    const publishWU = React.useCallback(async () => {
        if (queryName === "") {
            setQueryNameErrorMsg(nlsHPCC.ValidationErrorRequired);
            queryNameRef.current.focus();
        } else {
            setQueryNameErrorMsg("");

            const wu = await Workunit.create({ baseUrl: "" });

            await wu.update({ Jobname: queryName, QueryText: editor.ecl() });
            await wu.submit(cluster, WUUpdate.Action.Compile);

            wu.watchUntilComplete(changes => playgroundResults(wu, "publish"));
        }
    }, [cluster, editor, playgroundResults, queryName, setQueryNameErrorMsg]);

    const checkSyntax = React.useCallback(() => {
        service.WUSyntaxCheckECL({
            ECL: editor.ecl(),
            Cluster: cluster
        }).then(response => {
            if (response.Errors) {
                setSyntaxStatusIcon(SyntaxCheckResult.Failed);
                setSyntaxErrors(response.Errors.ECLException);
                displayErrors(null, editor, response.Errors.ECLException);
                setOutputMode(OutputMode.ERRORS);
            } else {
                setSyntaxStatusIcon(SyntaxCheckResult.Passed);
                setSyntaxErrors([]);
                displayErrors(null, editor, []);
            }
        });
    }, [cluster, editor, setOutputMode, setSyntaxErrors, setSyntaxStatusIcon]);

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
            {showSubmitBtn ? (
                <Button appearance="primary" onClick={submitWU}>{nlsHPCC.Submit}</Button>
            ) : (
                <Button appearance="primary" onClick={publishWU}>{nlsHPCC.Publish}</Button>
            )}
            <Button style={{ marginLeft: 6 }} onClick={checkSyntax} iconPosition="after"
                icon={
                    syntaxStatusIcon === SyntaxCheckResult.Passed ? <CheckmarkCircleRegular style={{ color: "green" }} /> :
                        syntaxStatusIcon === SyntaxCheckResult.Failed ? <DismissCircleRegular style={{ color: "red" }} /> :
                            <QuestionCircleRegular style={{ color: "initial" }} />
                }
            >
                {nlsHPCC.Syntax}
            </Button>
            <TargetClusterTextField
                key="target-cluster"
                label={nlsHPCC.Target}
                excludeRoxie={false}
                required={true}
                selectedKey={cluster}
                className={playgroundStyles.inlineDropdown}
                onChange={React.useCallback((evt, option: TargetClusterOption) => {
                    const selectedCluster = option.key.toString();
                    if (option?.queriesOnly) {
                        setShowSubmitBtn(false);
                    } else {
                        setShowSubmitBtn(true);
                    }
                    setCluster(selectedCluster);
                }, [setCluster])}
            />
            <div className={playgroundStyles.publishWrapper}>
                <TextField
                    label={nlsHPCC.Name}
                    name="jobName"
                    componentRef={queryNameRef}
                    required={!showSubmitBtn}
                    errorMessage={queryNameErrorMsg}
                    onChange={(evt, value) => setQueryName(value)}
                />
            </div>
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

    const { wuid, ecl, filter = {} } = props;
    const theme = useTheme();

    const [outputMode, setOutputMode] = React.useState<OutputMode>(OutputMode.ERRORS);
    const [workunit, setWorkunit] = React.useState<Workunit>();
    const [editor, setEditor] = React.useState<ECLEditor>();
    const [query, setQuery] = React.useState("");
    const [selectedEclSample, setSelectedEclSample] = React.useState("");
    const [eclContent, setEclContent] = React.useState("");
    const [syntaxErrors, setSyntaxErrors] = React.useState<any[]>([]);
    const [syntaxStatusIcon, setSyntaxStatusIcon] = React.useState(SyntaxCheckResult.Unknown);
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
        } else if (ecl) {
            setEclContent(ecl);
        }

        fetch("/esp/files/eclwatch/ecl/ECLPlaygroundSamples.json")
            .then(response => response.json())
            .then(json => setEclSamples(
                json.items.map(item => {
                    if (item.selected && !wuid && !ecl) {
                        setSelectedEclSample(item.filename);
                    }
                    return { key: item.filename, text: item.name };
                })
            ));

        if (editor) {
            if (theme.semanticColors.link === darkTheme.palette.themePrimary) {
                editor.option("theme", "darcula");
            } else {
                editor.option("theme", "default");
            }
        }
    }, [wuid, editor, theme, ecl]);

    React.useEffect(() => {
        fetch(`/esp/files/eclwatch/ecl/${selectedEclSample}`)
            .then(response => {
                if (response.status && response.status === 200 && response.body) {
                    response.text().then(sample => {
                        if (sample.toLowerCase().indexOf("<!doctype") < 0) {
                            setEclContent(sample);
                        }
                    });
                }
            });
    }, [selectedEclSample]);

    React.useEffect(() => {
        if (!editor) return;
        editor.ecl(eclContent);
    }, [editor, eclContent]);

    const handleThemeToggle = React.useCallback((evt) => {
        if (!editor) return;
        if (evt.detail && evt.detail.dark === true) {
            editor.option("theme", "darcula");
        } else {
            editor.option("theme", "default");
        }
    }, [editor]);
    useOnEvent(document, "eclwatch-theme-toggle", handleThemeToggle);

    const handleEclChange = React.useMemo(() => debounce((evt) => {
        if (editor.hasFocus()) {
            setSyntaxStatusIcon(SyntaxCheckResult.Unknown);
        }
    }, 300), [editor]);
    useOnEvent(window, "keyup", handleEclChange);

    return <div className={playgroundStyles.root}>
        <div className={playgroundStyles.titleBar}>
            <h1 className={playgroundStyles.title}>{nlsHPCC.title_ECLPlayground}</h1>
            <Dropdown
                label="Sample"
                className={`${playgroundStyles.inlineDropdown} ${playgroundStyles.samplesDropdown}`}
                options={eclSamples}
                selectedKey={selectedEclSample}
                placeholder="Select sample ECL..."
                onChange={(evt, item) => { setSelectedEclSample(item.key.toString()); }}
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
                                    editor={editor} setSyntaxErrors={setSyntaxErrors}
                                    syntaxStatusIcon={syntaxStatusIcon} setSyntaxStatusIcon={setSyntaxStatusIcon}
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
            <ReflexElement propagateDimensions={true} minSize={100} style={{ overflow: "hidden" }}>
                <div style={{ height: "calc(100% - 44px)" }}>
                    {outputMode === OutputMode.ERRORS ? (
                        <InfoGrid wuid={workunit?.Wuid} syntaxErrors={syntaxErrors} />

                    ) : outputMode === OutputMode.RESULTS ? (
                        <TabbedResults wuid={workunit?.Wuid} filter={filter} />

                    ) : outputMode === OutputMode.VIS ? (
                        <DojoAdapter widgetClassID="VizWidget" params={{ Wuid: workunit?.Wuid, Sequence: 0 }} />
                    ) : null}
                </div>
            </ReflexElement>
        </ReflexContainer>
    </div>;
};