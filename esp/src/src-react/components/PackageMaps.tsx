import * as React from "react";
import { CommandBar, ContextualMenuItemType, DefaultButton, Dropdown, ICommandBarItemProps, IDropdownOption, IStackTokens, Label, Link, mergeStyleSets, MessageBar, MessageBarType, Pivot, PivotItem, Stack } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { PackageProcessService } from "@hpcc-js/comms";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import { useConfirm } from "../hooks/confirm";
import { pivotItemStyle } from "../layouts/pivot";
import { pushParams, pushUrl } from "../util/history";
import { HolyGrail } from "../layouts/HolyGrail";
import { ReflexContainer, ReflexElement, ReflexSplitter } from "../layouts/react-reflex";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { ShortVerticalDivider } from "./Common";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { AddPackageMap } from "./forms/AddPackageMap";
import { selector } from "./DojoGrid";
import { TextSourceEditor, XMLSourceEditor } from "./SourceEditor";

const logger = scopedLogger("../components/PackageMaps.tsx");

const packageService = new PackageProcessService({ baseUrl: "" });

const FilterFields: Fields = {
    "Target": { type: "dropdown", label: nlsHPCC.Target, options: [], value: "*" },
    "Process": { type: "dropdown", label: nlsHPCC.Process, options: [], value: "*" },
    "ProcessFilter": { type: "dropdown", label: nlsHPCC.ProcessFilter, options: [], value: "*" },
};

const defaultUIState = {
    hasSelection: false
};

interface PackageMapsProps {
    filter?: { [key: string]: any };
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

const emptyFilter: { [key: string]: any } = {};

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
    if (!response?.Errors || response?.Errors.length < 1) {
        text += nlsHPCC.NoErrorFound;
    } else {
        text = addArrayToText(nlsHPCC?.Errors, response?.Errors, text);
    }
    if (!response.Warnings || response.Warnings.length < 1) {
        text += nlsHPCC.NoWarningFound;
    }
    else {
        text = addArrayToText(nlsHPCC.Warnings, response.Warnings, text);
    }
    text += "\n";
    text = addArrayToText(nlsHPCC.QueriesNoPackage, response?.queries.Unmatched, text);
    text = addArrayToText(nlsHPCC.PackagesNoQuery, response?.packages.Unmatched, text);
    text = addArrayToText(nlsHPCC.FilesNoPackage, response?.files.Unmatched, text);
    return text;
};

export type TypedDropdownOption = IDropdownOption & { type?: string };

export const PackageMaps: React.FunctionComponent<PackageMapsProps> = ({
    filter = emptyFilter,
    store,
    tab = "packageMaps"
}) => {

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);

    const [targets, setTargets] = React.useState<TypedDropdownOption[]>();
    const [processes, setProcesses] = React.useState<TypedDropdownOption[]>();
    const [activeMapTarget, setActiveMapTarget] = React.useState("");
    const [activeMapProcess, setActiveMapProcess] = React.useState("");
    const [contentsTarget, setContentsTarget] = React.useState("");
    const [contentsProcess, setContentsProcess] = React.useState("");

    const [processFilters, setProcessFilters] = React.useState<IDropdownOption[]>();
    const [showFilter, setShowFilter] = React.useState(false);
    const [showAddForm, setShowAddForm] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    const [showError, setShowError] = React.useState(false);
    const [errorMessage, setErrorMessage] = React.useState("");

    const [activeMapXml, setActiveMapXml] = React.useState("");
    const [activeMapValidationResult, setActiveMapValidationResult] = React.useState(nlsHPCC.ValidateResultHere);
    const [contentsXml, setContentsXml] = React.useState("");
    const [contentsValidationResult, setContentsValidationResult] = React.useState(nlsHPCC.ValidateResultHere);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    const changeActiveMapTarget = React.useCallback((evt, option) => {
        setActiveMapTarget(option.key.toString());
    }, [setActiveMapTarget]);
    const changeActiveMapProcess = React.useCallback((evt, option) => {
        setActiveMapProcess(option.key.toString());
    }, [setActiveMapProcess]);

    const changeContentsTarget = React.useCallback((evt, option) => {
        setContentsTarget(option.key.toString());
    }, [setContentsTarget]);
    const changeContentsProcess = React.useCallback((evt, option) => {
        setContentsProcess(option.key.toString());
    }, [setContentsProcess]);

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
        packageService.ValidatePackage({
            Target: activeMapTarget,
            Process: activeMapProcess,
            Info: activeMapXml
        })
            .then(({ Results }) => {
                setShowError(false);
                setActiveMapValidationResult(validateResponseToText(Results?.Result[0]));
            })
            .catch(err => logger.error(err));
    }, [activeMapProcess, activeMapTarget, activeMapXml, setActiveMapValidationResult]);

    const validateContents = React.useCallback(() => {
        setContentsValidationResult(nlsHPCC.Validating);
        packageService.ValidatePackage({
            Target: contentsTarget,
            Info: contentsXml
        })
            .then(({ Results }) => {
                setShowError(false);
                setContentsValidationResult(validateResponseToText(Results?.Result[0]));
            })
            .catch(err => logger.error(err));
    }, [contentsTarget, contentsXml, setContentsValidationResult]);

    React.useEffect(() => {
        if (activeMapProcess !== "" && activeMapTarget !== "") {
            packageService.GetPackage({ Target: activeMapTarget, Process: activeMapProcess })
                .then(({ Info }) => {
                    setActiveMapXml(Info);
                })
                .catch(err => logger.debug(err));
        }
    }, [activeMapProcess, activeMapTarget]);

    React.useEffect(() => {
        if (contentsProcess !== "" && contentsTarget !== "") {
            packageService.GetPackage({ Target: contentsTarget, Process: contentsProcess })
                .then(({ Info }) => {
                    setContentsXml(Info);
                })
                .catch(err => logger.debug(err));
        }
    }, [contentsProcess, contentsTarget]);

    const [data, setData] = React.useState<any[]>([]);

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
            col1: selector({
                width: 27,
                selectorType: "checkbox"
            }),
            Id: {
                label: nlsHPCC.PackageMap,
                formatter: (Id, row) => {
                    return <Link href={`#/packagemaps/${Id}`}>{Id}</Link>;
                }
            },
            Target: { label: nlsHPCC.Target },
            Process: { label: nlsHPCC.ProcessFilter },
            Active: {
                label: nlsHPCC.Active,
                formatter: (active) => {
                    if (active === true) {
                        return "A";
                    }
                    return "";
                }
            },
            Description: { label: nlsHPCC.Description }
        };
    }, []);

    const refreshData = React.useCallback(() => {
        packageService.ListPackages({
            Target: filter?.Target ?? "*",
            Process: filter?.Process ?? "*",
            ProcessFilter: filter?.ProcessFilter ?? "*"
        }).then(({ PackageMapList }) => {
            const packageMaps = PackageMapList?.PackageListMapData;
            if (packageMaps) {
                setData(packageMaps.map((packageMap, idx) => {
                    return {
                        Id: packageMap.Id,
                        Target: packageMap.Target,
                        Process: packageMap.Process,
                        Active: packageMap.Active,
                        Description: packageMap.Description
                    };
                }));
            }
        });
    }, [filter]);

    React.useEffect(() => refreshData(), [refreshData]);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedPackages,
        onSubmit: React.useCallback(() => {
            selection.forEach((item, idx) => {
                packageService.DeletePackage({
                    PackageMap: item.Id,
                    Target: item.Target,
                    Process: item.Process
                })
                    .then(({ status }) => {
                        if (status?.Code === 0) {
                            setShowError(false);
                            refreshData();
                        }
                    })
                    .catch(err => {
                        setShowError(true);
                        setErrorMessage(err.Exception[0].Message);
                        logger.debug(err);
                    });
            });
        }, [refreshData, selection])
    });

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection,
            onClick: () => {
                if (selection.length === 1) {
                    pushUrl(`/packagemaps/${selection[0]?.Id}`);
                } else {
                    selection.forEach(item => {
                        window.open(`/packagemaps/${item?.Id}`, "_blank");
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
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "activate", text: nlsHPCC.Activate, disabled: selection.length !== 1,
            onClick: () => {
                packageService.ActivatePackage({
                    Target: selection[0].Target,
                    Process: selection[0].Process,
                    PackageMap: selection[0].Id
                })
                    .then(({ status }) => {
                        if (status?.Code === 0) {
                            setShowError(false);
                            refreshData();
                        }
                    })
                    .catch(err => logger.debug(err))
                    ;
            }
        },
        {
            key: "deactivate", text: nlsHPCC.Deactivate, disabled: selection.length !== 1,
            onClick: () => {
                packageService.DeActivatePackage({
                    Target: selection[0].Target,
                    Process: selection[0].Process,
                    PackageMap: selection[0].Id
                })
                    .then(({ status }) => {
                        if (status?.Code === 0) {
                            setShowError(false);
                            refreshData();
                        }
                    })
                    .catch(err => {
                        setShowError(true);
                        setErrorMessage(err.Exception[0].Message);
                        logger.debug(err);
                    });
            }
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "filter", text: nlsHPCC.Filter, disabled: !!store, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
            onClick: () => {
                setShowFilter(true);
            }
        },
    ], [hasFilter, refreshData, selection, setShowDeleteConfirm, store, uiState.hasSelection]);

    const copyButtons = useCopyButtons(columns, selection, "packageMaps");

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
        packageService
            .GetPackageMapSelectOptions({ IncludeProcesses: true })
            .then(({ ProcessFilters, Targets }) => {
                setProcessFilters(ProcessFilters?.Item.map(item => {
                    return { key: item, text: item };
                }));
                const _targets: TypedDropdownOption[] = [{ key: "*", text: "ANY" }];
                const _processes: TypedDropdownOption[] = [{ key: "*", text: "ANY" }];
                Targets?.TargetData.map(target => {
                    if (_targets.filter(t => t.key === target.Name).length === 0) {
                        _targets.push({ key: target.Name, text: target.Name, type: target.Type });
                    }
                    target?.Processes?.Item.map(item => {
                        if (_processes.filter(p => p.key === item).length === 0) {
                            _processes.push({ key: item, text: item, type: target.Type });
                        }
                    });
                });

                setTargets(_targets);
                setProcesses(_processes);
            })
            .catch(err => logger.debug(err))
            ;
    }, []);

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
                        header={<CommandBar items={buttons} farItems={copyButtons} />}
                        main={<FluentGrid
                            data={data}
                            primaryID={"Id"}
                            sort={{ attribute: "Id", descending: true }}
                            columns={columns}
                            setSelection={setSelection}
                            setTotal={setTotal}
                            refresh={refreshTable}
                        ></FluentGrid>}
                    />
                    <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
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
                                    <Label>{nlsHPCC.Process}</Label>
                                    <Dropdown
                                        id="contentsProcess" className={validateMapStyles.dropdown}
                                        options={processes} selectedKey={contentsProcess} placeholder={nlsHPCC.SelectEllipsis}
                                        onChange={changeContentsProcess}
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
            refreshData={refreshData} targets={targets} processes={processes}
        />
        <DeleteConfirm />
    </>;
};