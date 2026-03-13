import * as React from "react";
import { Button, Dropdown, Field, Input, Label, Link, makeStyles, mergeClasses, Option, Spinner } from "@fluentui/react-components";
import { useOnEvent } from "@fluentui/react-hooks";
import { CheckmarkCircleRegular, DataBarVerticalFilled, DismissCircleRegular, QuestionCircleRegular, TableRegular, WarningRegular } from "@fluentui/react-icons";
import { Workunit, WUUpdate, WorkunitsService, WUStateID } from "@hpcc-js/comms";
import { ECLEditor, IPosition } from "@hpcc-js/codemirror";
import { scopedLogger } from "@hpcc-js/util";
import { useUserTheme } from "../hooks/theme";
import { DockPanel, DockPanelItem, ResetableDockPanel } from "../layouts/DockPanel";
import { DojoAdapter } from "../layouts/DojoAdapter";
import { HolyGrail } from "../layouts/HolyGrail";
import { AutosizeComponent } from "../layouts/HpccJSAdapter";
import { pushUrl } from "../util/history";
import { debounce } from "../util/throttle";
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

const useStyles = makeStyles({
    root: {
        height: "100%",
        border: borderStyle,
        "& .fui-Button": {
            height: "min-content"
        },
        "& .fui-Label::after": {
            paddingRight: "0"
        },
        "& .fui-Label": {
            lineHeight: "32px",
            margin: "0 10px",
            padding: "0"
        }
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
        margin: "0",
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
    targetCluster: {
        display: "flex",
    },
    inlineDropdown: {
        display: "flex",
        maxHeight: "32px",
        minWidth: "100px",
    },
    samplesWrapper: {
        display: "flex"
    },
    samplesDropdown: {
        "& .ms-Dropdown": {
            width: "240px"
        }
    },
    publishWrapper: {
        display: "flex",
        "& .ms-TextField-wrapper": {
            display: "flex",
            marginLeft: "18px"
        },
        "& .ms-TextField-errorMessage": {
            display: "none"
        }
    },
    outputButtons: {
        display: "flex",
        marginLeft: "18px"
    },
    statusMessage: {
        alignSelf: "center",
        minWidth: "100px"
    },
    fullscreen: {
        position: "absolute",
        top: "0",
        left: "0",
        width: "100%",
        height: "100%",
        backgroundColor: "#fff"
    },
});

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

    const styles = useStyles();
    const [cluster, setCluster] = React.useState("");
    const [wuState, setWuState] = React.useState("");
    const [queryName, setQueryName] = React.useState("");
    const queryNameRef = React.useRef<HTMLInputElement | null>(null);
    const [queryNameErrorMsg, setQueryNameErrorMsg] = React.useState("");
    const [showSubmitBtn, setShowSubmitBtn] = React.useState(true);

    const playgroundResults = React.useCallback((wu, action = "submit") => {
        setWuState(wu.State);
        if (document.location.hash.includes("play")) {
            if (wu.isFailed()) {
                pushUrl(`/play/${wu.Wuid}`);
                displayErrors(wu, editor);
                setOutputMode(OutputMode.ERRORS);
            } else if (wu.isComplete()) {
                pushUrl(`/play/${wu.Wuid}`);
                if (action === "publish") {
                    wu.publish(queryName);
                    setWuState("Published");
                }
                setOutputMode(OutputMode.RESULTS);
            }
        } else {
            if (wu.isComplete()) {
                logger.info(`${nlsHPCC.Playground} ${nlsHPCC.Finished} (${wu.Wuid})`);
            }
        }
    }, [editor, queryName, setOutputMode]);

    const submitWU = React.useCallback(async () => {
        const wu = await Workunit.create({ baseUrl: "" });
        setWorkunit(wu);

        await wu.update({ Jobname: queryName, QueryText: editor.ecl() });
        await wu.submit(cluster);

        wu.watchUntilComplete(changes => playgroundResults(wu));
    }, [cluster, editor, playgroundResults, queryName, setWorkunit]);

    const publishWU = React.useCallback(async () => {
        if (queryName === "") {
            setQueryNameErrorMsg(nlsHPCC.ValidationErrorRequired);
            queryNameRef.current.focus();
        } else {
            setQueryNameErrorMsg("");

            const wu = await Workunit.create({ baseUrl: "" });
            setWorkunit(wu);

            await wu.update({ Jobname: queryName, QueryText: editor.ecl() });
            await wu.submit(cluster, WUUpdate.Action.Compile);

            wu.watchUntilComplete(changes => playgroundResults(wu, "publish"));
        }
    }, [cluster, editor, playgroundResults, queryName, setQueryNameErrorMsg, setWorkunit]);

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

    return <div className={styles.toolBar}>
        <div className={styles.controlsWrapper}>
            {showSubmitBtn ? (
                <Button appearance="primary" onClick={submitWU}>{nlsHPCC.Submit}</Button>
            ) : (
                <Button appearance="primary" onClick={publishWU}>{nlsHPCC.Publish}</Button>
            )}
            <Button style={{ marginLeft: 6 }} onClick={checkSyntax} iconPosition="after"
                icon={
                    syntaxStatusIcon === SyntaxCheckResult.Passed ? <CheckmarkCircleRegular style={{ color: "green" }} /> :
                        syntaxStatusIcon === SyntaxCheckResult.Failed ? <DismissCircleRegular style={{ color: "red" }} /> :
                            <QuestionCircleRegular style={{ color: "inherit" }} />
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
                className={styles.inlineDropdown}
                fieldClass={styles.targetCluster}
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
            <div className={styles.publishWrapper}>
                <Label>{nlsHPCC.Name}</Label>
                <Field validationMessage={queryNameErrorMsg} validationState={queryNameErrorMsg ? "error" : "none"}>
                    <Input
                        name="jobName"
                        ref={queryNameRef}
                        required={!showSubmitBtn}
                        onChange={(evt, data) => setQueryName(data.value)}
                    />
                </Field>
            </div>
            <div className={styles.outputButtons}>
                <Button
                    icon={<WarningRegular />}
                    aria-label={nlsHPCC.ErrorWarnings}
                    onClick={React.useCallback(() => setOutputMode(OutputMode.ERRORS), [setOutputMode])}
                    appearance={outputMode === OutputMode.ERRORS ? undefined : "subtle"}
                />
                <Button
                    icon={<TableRegular />}
                    aria-label={nlsHPCC.Results}
                    onClick={React.useCallback(() => setOutputMode(OutputMode.RESULTS), [setOutputMode])}
                    appearance={outputMode === OutputMode.RESULTS ? undefined : "subtle"}
                    disabled={workunit?.Wuid ? false : true}
                />
                <Button
                    icon={<DataBarVerticalFilled />}
                    aria-label={nlsHPCC.Visualizations}
                    onClick={React.useCallback(() => setOutputMode(OutputMode.VIS), [setOutputMode])}
                    appearance={outputMode === OutputMode.VIS ? undefined : "subtle"}
                    disabled={workunit?.Wuid ? false : true}
                />
            </div>
        </div>
        <div className={styles.statusMessage}>
            <Link href={(workunit?.Wuid) ? `#/workunits/${workunit.Wuid}` : ""}>{wuState}</Link>
        </div>
    </div>;
};

export const ECLPlayground: React.FunctionComponent<ECLPlaygroundProps> = (props) => {

    const { wuid, ecl, filter = {} } = props;
    const { isDark } = useUserTheme();
    const styles = useStyles();

    const [outputMode, setOutputMode] = React.useState<OutputMode>(OutputMode.ERRORS);
    const [dockpanel, setDockpanel] = React.useState<ResetableDockPanel>();
    const [workunit, setWorkunit] = React.useState<Workunit>();
    const [editor, setEditor] = React.useState<ECLEditor>();
    const [query, setQuery] = React.useState("");
    const [selectedEclSample, setSelectedEclSample] = React.useState("");
    const [eclContent, setEclContent] = React.useState("");
    const [syntaxErrors, setSyntaxErrors] = React.useState<any[]>([]);
    const [syntaxStatusIcon, setSyntaxStatusIcon] = React.useState(SyntaxCheckResult.Unknown);
    const [eclSamples, setEclSamples] = React.useState<{ key: string, text: string }[]>([]);

    React.useEffect(() => {
        if (wuid) {
            const wu = Workunit.attach({ baseUrl: "" }, wuid);
            wu.fetchQuery().then(result => {
                setWorkunit(wu);
                setQuery(result.Text);
                setEclContent(result.Text);
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
            editor.option("theme", isDark ? "darcula" : "default");
        }
    }, [wuid, editor, ecl, isDark]);

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
        if (dockpanel) {
            //  Should only happen once on startup  ---
            const layout: any = dockpanel.layout();
            if (Array.isArray(layout?.main?.sizes) && layout.main.sizes.length === 2) {
                layout.main.sizes = [0.7, 0.3];
                dockpanel.layout(layout).lazyRender();
            }
        }
    }, [dockpanel]);

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

    const submissionComplete = React.useMemo(() => {
        if (!workunit?.Wuid) return true;
        return workunit?.StateID === WUStateID.Completed ||
            workunit?.StateID === WUStateID.Failed ||
            (workunit?.ActionEx === "compile" && workunit?.StateID === WUStateID.Compiled);
    }, [workunit?.StateID, workunit?.ActionEx, workunit?.Wuid]);

    const handleEclChange = React.useMemo(() => debounce((evt) => {
        if (editor.hasFocus()) {
            setSyntaxStatusIcon(SyntaxCheckResult.Unknown);
        }
    }, 300), [editor]);
    useOnEvent(window, "keyup", handleEclChange);

    return <div className={styles.root}>
        <HolyGrail
            header={
                <div className={styles.titleBar}>
                    <h1 className={styles.title}>{nlsHPCC.title_ECLPlayground}</h1>
                    <Field className={styles.samplesWrapper} label={nlsHPCC.Sample}>
                        <Dropdown
                            id="eclSamples"
                            className={mergeClasses(styles.inlineDropdown, styles.samplesDropdown)}
                            selectedOptions={[selectedEclSample]}
                            placeholder="Select sample ECL..."
                            onOptionSelect={(evt, data) => { setSelectedEclSample(data.optionValue.toString()); }}
                        >
                            {eclSamples.map((sample, idx) => (
                                <Option key={`eclSample_${idx}`} text={sample.text} value={sample.key.toString()}>{sample.text}</Option>
                            ))}
                        </Dropdown>
                    </Field>
                </div>
            }
            main={
                <DockPanel hideSingleTabs onCreate={setDockpanel}>
                    <DockPanelItem key="eclEditor" title={nlsHPCC.ECL}>
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
                    </DockPanelItem>
                    <DockPanelItem key="graph" title={nlsHPCC.Graphs} location="split-right" relativeTo="eclEditor">
                        {submissionComplete ?
                            <AutosizeComponent>
                                <DojoAdapter widgetClassID="Graph7Widget" params={{ Wuid: workunit?.Wuid }} />
                            </AutosizeComponent>
                            : <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%" }}>
                                <Spinner size="large" />
                            </div>
                        }
                    </DockPanelItem>
                    <DockPanelItem key="output" title={nlsHPCC.Outputs} location="split-bottom" relativeTo="eclEditor">
                        {submissionComplete ?
                            (outputMode === OutputMode.ERRORS ? (
                                <InfoGrid wuid={workunit?.Wuid} syntaxErrors={syntaxErrors} />
                            ) : outputMode === OutputMode.RESULTS ? (
                                <TabbedResults wuid={workunit?.Wuid} filter={filter} />
                            ) : outputMode === OutputMode.VIS ? (
                                <DojoAdapter widgetClassID="VizWidget" params={{ Wuid: workunit?.Wuid, Sequence: 0 }} />
                            ) : null)
                            : <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%" }}>
                                <Spinner size="large" />
                            </div>
                        }
                    </DockPanelItem>
                </DockPanel>
            }
        />
    </div>;
};