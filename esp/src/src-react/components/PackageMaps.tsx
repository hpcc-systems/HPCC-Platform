import * as React from "react";
import { CommandBar, ContextualMenuItemType, DefaultButton, Dropdown, ICommandBarItemProps, IDropdownOption, IStackTokens, Label, mergeStyleSets, MessageBar, MessageBarType, Pivot, PivotItem, Stack } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import { scopedLogger } from "@hpcc-js/util";
import { SizeMe } from "react-sizeme";
import * as ESPPackageProcess from "src/ESPPackageProcess";
import * as WsPackageMaps from "src/WsPackageMaps";
import nlsHPCC from "src/nlsHPCC";
import { pivotItemStyle } from "../layouts/pivot";
import { pushParams, pushUrl } from "../util/history";
import { ShortVerticalDivider } from "./Common";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { AddPackageMap } from "./forms/AddPackageMap";
import { DojoGrid, selector } from "./DojoGrid";
import { HolyGrail } from "../layouts/HolyGrail";
import { ReflexContainer, ReflexElement, ReflexSplitter } from "../layouts/react-reflex";
import { TextSourceEditor, XMLSourceEditor } from "./SourceEditor";

const logger = scopedLogger("../components/PackageMaps.tsx");

const FilterFields: Fields = {
    "Target": { type: "dropdown", label: nlsHPCC.Target, options: [], value: "*" },
    "Process": { type: "dropdown", label: nlsHPCC.Process, options: [], value: "*" },
    "ProcessFilter": { type: "dropdown", label: nlsHPCC.ProcessFilter, options: [], value: "*" },
};

function formatQuery(filter) {
    return filter;
}

const defaultUIState = {
    hasSelection: false
};

interface PackageMapsProps {
    filter?: object;
    store?: any;
    tab?: string;
}

const validateMapStackTokens: IStackTokens = {
    childrenGap: 10,
    padding: "5px 0"
};

const validateMapStyles = mergeStyleSets({
    dropdown: { minWidth: 140, marginLeft: 20 },
    displayNone: { display: "none" }
});

const emptyFilter = {};

const addArrayToText = (title, items, text): string => {
    if ((items.Item !== undefined) && (items.Item.length > 0)) {
        text += title + ":\n";
        for (let i = 0; i < items.Item.length; i++)
            text += "  " + items.Item[i] + "\n";
        text += "\n";
    }
    return text;
};

const validateResponseToText = (response): string => {
    let text = "";
    if (!response.Errors || response.Errors.length < 1) {
        text += nlsHPCC.NoErrorFound;
    } else {
        text = addArrayToText(nlsHPCC.Errors, response.Errors, text);
    }
    if (!response.Warnings || response.Warnings.length < 1) {
        text += nlsHPCC.NoWarningFound;
    }
    else {
        text = addArrayToText(nlsHPCC.Warnings, response.Warnings, text);
    }
    text += "\n";
    text = addArrayToText(nlsHPCC.QueriesNoPackage, response.queries.Unmatched, text);
    text = addArrayToText(nlsHPCC.PackagesNoQuery, response.packages.Unmatched, text);
    text = addArrayToText(nlsHPCC.FilesNoPackage, response.files.Unmatched, text);
    return text;
};

export const PackageMaps: React.FunctionComponent<PackageMapsProps> = ({
    filter = emptyFilter,
    store,
    tab = "packageMaps"
}) => {

    const [grid, setGrid] = React.useState<any>(undefined);

    const [targets, setTargets] = React.useState<IDropdownOption[]>();
    const [processes, setProcesses] = React.useState<IDropdownOption[]>();
    const [activeMapTarget, setActiveMapTarget] = React.useState("");
    const [activeMapProcess, setActiveMapProcess] = React.useState("");
    const [contentsTarget, setContentsTarget] = React.useState("");

    const [processFilters, setProcessFilters] = React.useState<IDropdownOption[]>();
    const [showFilter, setShowFilter] = React.useState(false);
    const [showAddForm, setShowAddForm] = React.useState(false);
    const [selection, setSelection] = React.useState([]);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");

    const [activeMapXml, setActiveMapXml] = React.useState("");
    const [activeMapValidationResult, setActiveMapValidationResult] = React.useState(nlsHPCC.ValidateResultHere);
    const [contentsXml, setContentsXml] = React.useState("");
    const [contentsValidationResult, setContentsValidationResult] = React.useState(nlsHPCC.ValidateResultHere);

    const changeActiveMapTarget = React.useCallback((evt, option) => {
        setActiveMapTarget(option.key.toString());
    }, [setActiveMapTarget]);
    const changeActiveMapProcess = React.useCallback((evt, option) => {
        setActiveMapProcess(option.key.toString());
    }, [setActiveMapProcess]);

    const changeContentsTarget = React.useCallback((evt, option) => {
        setContentsTarget(option.key.toString());
    }, [setContentsTarget]);

    const handleFileSelect = React.useCallback((evt) => {
        evt.preventDefault();
        evt.stopPropagation();
        const file = evt.target.files[0];
        const fileReader = new FileReader();
        fileReader.onload = function (evt) { setContentsXml(evt.target.result.toString()); };
        fileReader.readAsText(file);
    }, [setContentsXml]);

    const handleLoadMapFromFileClick = React.useCallback(() => document.getElementById("uploadMapFromFile").click(), []);

    const validateActiveMap = React.useCallback(() => {
        setActiveMapValidationResult(nlsHPCC.Validating);
        WsPackageMaps.validatePackage({
            request: {
                Target: activeMapTarget,
                Info: activeMapXml
            }
        })
            .then(({ ValidatePackageResponse, Exceptions }) => {
                if (Exceptions?.Exception.length > 0) {
                    setShowError(true);
                    setErrorMessage(Exceptions?.Exception[0].Message);
                } else {
                    setActiveMapValidationResult(validateResponseToText(ValidatePackageResponse));
                }
            })
            .catch(logger.error)
            ;
    }, [activeMapTarget, activeMapXml, setActiveMapValidationResult, setErrorMessage, setShowError]);

    const validateContents = React.useCallback(() => {
        setContentsValidationResult(nlsHPCC.Validating);
        WsPackageMaps.validatePackage({
            request: {
                Target: contentsTarget,
                Info: contentsXml
            }
        })
            .then(({ ValidatePackageResponse, Exceptions }) => {
                if (Exceptions?.Exception.length > 0) {
                    setShowError(true);
                    setErrorMessage(Exceptions?.Exception[0].Message);
                } else {
                    setContentsValidationResult(validateResponseToText(ValidatePackageResponse));
                }
            })
            .catch(logger.error)
            ;
    }, [contentsTarget, contentsXml, setContentsValidationResult, setErrorMessage, setShowError]);

    React.useEffect(() => {
        if (activeMapProcess !== "" && activeMapTarget !== "") {
            WsPackageMaps.getPackage({ target: activeMapTarget, process: activeMapProcess })
                .then(({ GetPackageResponse }) => {
                    setActiveMapXml(GetPackageResponse.Info);
                })
                .catch(logger.error)
                ;
        }
    }, [activeMapProcess, activeMapTarget]);

    //  Grid ---
    const gridStore = useConst(store || ESPPackageProcess.CreatePackageMapQueryObjectStore({}));
    const gridQuery = useConst(formatQuery(filter));
    const gridColumns = useConst({
        col1: selector({
            width: 27,
            selectorType: "checkbox"
        }),
        Id: {
            label: nlsHPCC.PackageMap,
            formatter: function (Id, idx) {
                return `<a href="#/packagemaps/${Id}" class='dgrid-row-url'>${Id}</a>`;
            }
        },
        Target: { label: nlsHPCC.Target },
        Process: { label: nlsHPCC.ProcessFilter },
        Active: {
            label: nlsHPCC.Active,
            formatter: function (active) {
                if (active === true) {
                    return "A";
                }
                return "";
            }
        },
        Description: { label: nlsHPCC.Description }
    });

    const refreshTable = React.useCallback((clearSelection = false) => {
        grid?.set("query", formatQuery(filter));
        if (clearSelection) {
            grid?.clearSelection();
        }
    }, [filter, grid]);

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection,
            onClick: () => {
                if (selection.length === 1) {
                    pushUrl(`#/packagemaps/${selection[0]?.Id}`);
                } else {
                    selection.forEach(item => {
                        window.open(`#/packagemaps/${item?.Id}`, "_blank");
                    });
                }
            }
        },
        {
            key: "add", text: nlsHPCC.Add,
            onClick: () => setShowAddForm(true)
        },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.hasSelection,
            onClick: () => {
                if (confirm(nlsHPCC.DeleteSelectedPackages)) {
                    selection.forEach((item, idx) => {
                        WsPackageMaps.deletePackageMap({
                            request: {
                                PackageMap: item.Id,
                                Target: item.Target,
                                Process: item.Process
                            }
                        })
                            .then(({ DeletePackageResponse, Exceptions }) => {
                                if (DeletePackageResponse?.status?.Code === 0) {
                                    refreshTable();
                                } else if (Exceptions?.Exception.length > 0) {
                                    setShowError(true);
                                    setErrorMessage(Exceptions?.Exception[0].Message);
                                }
                            })
                            .catch(logger.error)
                            ;
                    });
                }
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "activate", text: nlsHPCC.Activate, disabled: selection.length !== 1,
            onClick: () => {
                WsPackageMaps.activatePackageMap({
                    request: {
                        Target: selection[0].Target,
                        Process: selection[0].Process,
                        PackageMap: selection[0].Id
                    }
                })
                    .then(({ ActivatePackageResponse }) => {
                        if (ActivatePackageResponse?.status?.Code === 0) {
                            refreshTable();
                        }
                    })
                    .catch(logger.error)
                    ;
            }
        },
        {
            key: "deactivate", text: nlsHPCC.Deactivate, disabled: selection.length !== 1,
            onClick: () => {
                WsPackageMaps.deactivatePackageMap({
                    request: {
                        Target: selection[0].Target,
                        Process: selection[0].Process,
                        PackageMap: selection[0].Id
                    }
                })
                    .then(({ DeActivatePackageResponse, Exceptions }) => {
                        if (DeActivatePackageResponse?.status?.Code === 0) {
                            refreshTable();
                        } else if (Exceptions?.Exception.length > 0) {
                            setShowError(true);
                            setErrorMessage(Exceptions?.Exception[0].Message);
                        }
                    })
                    .catch(logger.error)
                    ;
            }
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "filter", text: nlsHPCC.Filter, disabled: !!store, iconProps: { iconName: "Filter" },
            onClick: () => {
                setShowFilter(true);
            }
        },
    ], [refreshTable, selection, store, uiState.hasSelection]);

    //  Filter  ---
    const filterFields: Fields = {};
    for (const field in FilterFields) {
        filterFields[field] = { ...FilterFields[field], value: filter[field] ? filter[field] : FilterFields[field].value };
        switch (field) {
            case "Target":
                filterFields[field]["options"] = targets;
                break;
            case "Process":
                filterFields[field]["options"] = processes;
                break;
            case "ProcessFilter":
                filterFields[field]["options"] = processFilters;
                break;
        }
    }

    React.useEffect(() => {
        WsPackageMaps
            .GetPackageMapSelectTargets({ request: { IncludeProcesses: true } })
            .then(({ GetPackageMapSelectOptionsResponse }) => {
                setProcessFilters(GetPackageMapSelectOptionsResponse?.ProcessFilters?.Item.map(item => {
                    return { key: item, text: item };
                }));
                const _targets = [{ key: "*", text: "ANY" }];
                const _processes = [{ key: "*", text: "ANY" }];
                GetPackageMapSelectOptionsResponse?.Targets?.TargetData.map(target => {
                    if (_targets.filter(t => t.key === target.Type).length === 0) {
                        _targets.push({ key: target.Type, text: target.Type });
                    }
                    target?.Processes?.Item.map(item => {
                        if (_processes.filter(p => p.key === item).length === 0) {
                            _processes.push({ key: item, text: item });
                        }
                    });
                });

                setTargets(_targets);
                setProcesses(_processes);
            })
            .catch(logger.error)
            ;
    }, []);

    React.useEffect(() => {
        refreshTable();
    }, [filter, refreshTable]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        for (let i = 0; i < selection.length; ++i) {
            state.hasSelection = true;
        }
        setUIState(state);
    }, [selection]);

    return <>
        {showError &&
            <MessageBar messageBarType={MessageBarType.error} isMultiline={false} onDismiss={() => setShowError(false)} dismissButtonAriaLabel="Close">
                {errorMessage}
            </MessageBar>
        }
        <SizeMe monitorHeight>{({ size }) =>
            <Pivot
                overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab}
                onLinkClick={evt => {
                    if (["activeMap", "packageContents"].indexOf(evt.props.itemKey) > -1) {
                        pushUrl(`/packagemaps/validate/${evt.props.itemKey}`);
                    } else {
                        if (evt.props.itemKey === "list") {
                            pushUrl("/packagemaps");
                        } else {
                            pushUrl(`/packagemaps/${evt.props.itemKey}`);
                        }
                    }
                }}
            >
                <PivotItem headerText={nlsHPCC.PackageMaps} itemKey="list" style={pivotItemStyle(size)} >
                    <HolyGrail
                        header={<CommandBar items={buttons} />}
                        main={
                            <>
                                <DojoGrid store={gridStore} query={gridQuery} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />
                                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
                            </>
                        }
                    />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.ValidateActivePackageMap} itemKey="activeMap" style={pivotItemStyle(size, 0)}>
                    <HolyGrail
                        header={
                            <Stack horizontal tokens={validateMapStackTokens}>
                                <Stack horizontal tokens={validateMapStackTokens}>
                                    <Label>{nlsHPCC.Target}</Label>
                                    <Dropdown
                                        id="activeMapTarget" className={validateMapStyles.dropdown}
                                        options={targets} placeholder={nlsHPCC.SelectEllipsis}
                                        onChange={changeActiveMapTarget}
                                    />
                                </Stack>
                                <Stack horizontal tokens={validateMapStackTokens}>
                                    <Label>{nlsHPCC.Process}</Label>
                                    <Dropdown
                                        id="activeMapProcess" className={validateMapStyles.dropdown}
                                        options={processes} placeholder={nlsHPCC.SelectEllipsis}
                                        onChange={changeActiveMapProcess}
                                    />
                                </Stack>
                                <Stack horizontal tokens={validateMapStackTokens}>
                                    <DefaultButton id="validateMap" text={nlsHPCC.Validate} onClick={validateActiveMap} />
                                </Stack>
                            </Stack>
                        }
                        main={
                            <ReflexContainer orientation="vertical">
                                <ReflexElement>
                                    <XMLSourceEditor text={activeMapXml} readonly={true} />
                                </ReflexElement>
                                <ReflexSplitter />
                                <ReflexElement>
                                    <TextSourceEditor text={activeMapValidationResult} readonly={true} />
                                </ReflexElement>
                            </ReflexContainer>
                        }
                    />
                </PivotItem>
                <PivotItem headerText={nlsHPCC.ValidatePackageContent} itemKey="packageContents" style={pivotItemStyle(size, 0)}>
                    <HolyGrail
                        header={
                            <Stack horizontal tokens={validateMapStackTokens}>
                                <Stack horizontal tokens={validateMapStackTokens}>
                                    <Label>{nlsHPCC.Target}</Label>
                                    <Dropdown
                                        id="contentsTarget" className={validateMapStyles.dropdown}
                                        options={targets} selectedKey={contentsTarget} placeholder={nlsHPCC.SelectEllipsis}
                                        onChange={changeContentsTarget}
                                    />
                                </Stack>
                                <Stack horizontal tokens={validateMapStackTokens}>
                                    <input id="uploadMapFromFile" type="file" className={validateMapStyles.displayNone} accept="*.xml" onChange={handleFileSelect} />
                                    <DefaultButton
                                        id="loadMapFromFile" text={nlsHPCC.LoadPackageFromFile}
                                        onClick={handleLoadMapFromFileClick}
                                    />
                                    <DefaultButton id="validateMap" text={nlsHPCC.Validate} onClick={validateContents} />
                                </Stack>
                            </Stack>
                        }
                        main={
                            <ReflexContainer orientation="vertical">
                                <ReflexElement>
                                    <XMLSourceEditor text={contentsXml} readonly={true} />
                                </ReflexElement>
                                <ReflexSplitter />
                                <ReflexElement>
                                    <TextSourceEditor text={contentsValidationResult} readonly={true} />
                                </ReflexElement>
                            </ReflexContainer>
                        }
                    />
                </PivotItem>
            </Pivot>
        }</SizeMe>
        <AddPackageMap
            showForm={showAddForm} setShowForm={setShowAddForm}
            refreshTable={refreshTable} targets={targets} processes={processes}
        />
    </>;
};