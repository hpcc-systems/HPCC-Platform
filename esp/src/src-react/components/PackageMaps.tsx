import * as React from "react";
import { IDropdownOption } from "./forms/Fields";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "./CommandBarV9";
import { Button, Dropdown, Label, Link, MessageBar, MessageBarActions, MessageBarBody, Option, makeStyles, SelectTabData, SelectTabEvent, Tab, TabList } from "@fluentui/react-components";
import { DismissRegular } from "@fluentui/react-icons";
import { scopedLogger } from "@hpcc-js/util";
import { PackageProcessService } from "@hpcc-js/comms";
import { SizeMe } from "../layouts/SizeMe";
import nlsHPCC from "src/nlsHPCC";
import { useConfirm } from "../hooks/confirm";
import { pivotItemStyle } from "../layouts/pivot";
import { pushParams, pushUrl } from "../util/history";
import { HolyGrail } from "../layouts/HolyGrail";
import { ReflexContainer, ReflexElement, ReflexSplitter } from "../layouts/react-reflex";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { AddPackageMap } from "./forms/AddPackageMap";
import { selector } from "./DojoGrid";
import { TextSourceEditor, XMLSourceEditor } from "./SourceEditor";

const logger = scopedLogger("../components/PackageMaps.tsx");

const useStyles = makeStyles({
    container: {
        height: "100%",
        position: "relative"
    }
});

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

const validateMapStackStyle: React.CSSProperties = {
    display: "flex",
    flexDirection: "row",
    gap: "10px",
    padding: "5px 0"
};

const useValidateMapStyles = makeStyles({
    dropdown: { minWidth: "140px", marginLeft: "20px" },
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
    tab = "list"
}) => {

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);
    const validateMapStyles = useValidateMapStyles();

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

    const changeActiveMapTarget = React.useCallback((_evt, data) => {
        setActiveMapTarget(String(data.optionValue));
    }, [setActiveMapTarget]);
    const changeActiveMapProcess = React.useCallback((_evt, data) => {
        setActiveMapProcess(String(data.optionValue));
    }, [setActiveMapProcess]);

    const changeContentsTarget = React.useCallback((_evt, data) => {
        setContentsTarget(String(data.optionValue));
    }, [setContentsTarget]);
    const changeContentsProcess = React.useCallback((_evt, data) => {
        setContentsProcess(String(data.optionValue));
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
        { key: "divider_1", itemType: ContextualMenuItemType.Divider },
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
        { key: "divider_2", itemType: ContextualMenuItemType.Divider },
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
        { key: "divider_3", itemType: ContextualMenuItemType.Divider },
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
            .GetPackageMapSelectOptions({ IncludeProcesses: true, IncludeProcessFilters: true })
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

    const onTabSelect = React.useCallback((_: SelectTabEvent, data: SelectTabData) => {
        const nextTab = data.value as string;
        if (["activeMap", "packageContents"].includes(nextTab)) {
            pushUrl(`/packagemaps/validate/${nextTab}`);
        } else {
            if (nextTab === "list") {
                pushUrl("/packagemaps");
            } else {
                pushUrl(`/packagemaps/${nextTab}`);
            }
        }
    }, []);

    const styles = useStyles();

    return <>
        {showError &&
            <MessageBar intent="error">
                <MessageBarBody>{errorMessage}</MessageBarBody>
                <MessageBarActions containerAction={<Button onClick={() => setShowError(false)} aria-label="Close" appearance="transparent" icon={<DismissRegular />} />} />
            </MessageBar>
        }
        <SizeMe>{({ size }) =>
            <div className={styles.container}>
                <TabList selectedValue={tab} onTabSelect={onTabSelect} size="medium">
                    <Tab value="list">{nlsHPCC.PackageMaps}</Tab>
                    <Tab value="activeMap">{nlsHPCC.ValidateActivePackageMap}</Tab>
                    <Tab value="packageContents">{nlsHPCC.ValidatePackageContent}</Tab>
                </TabList>
                {tab === "list" &&
                    <div style={pivotItemStyle(size)}>
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
                    </div>
                }
                {tab === "activeMap" &&
                    <div style={pivotItemStyle(size, 0)}>
                        <HolyGrail
                            header={
                                <div style={validateMapStackStyle}>
                                    <div style={validateMapStackStyle}>
                                        <Label>{nlsHPCC.Target}</Label>
                                        <Dropdown
                                            id="activeMapTarget" className={validateMapStyles.dropdown}
                                            placeholder={nlsHPCC.SelectEllipsis}
                                            onOptionSelect={changeActiveMapTarget}
                                        >
                                            {targets?.map(opt => (
                                                <Option key={String(opt.key)} text={opt.text} value={String(opt.key)}>{opt.text}</Option>
                                            ))}
                                        </Dropdown>
                                    </div>
                                    <div style={validateMapStackStyle}>
                                        <Label>{nlsHPCC.Process}</Label>
                                        <Dropdown
                                            id="activeMapProcess" className={validateMapStyles.dropdown}
                                            placeholder={nlsHPCC.SelectEllipsis}
                                            onOptionSelect={changeActiveMapProcess}
                                        >
                                            {processes?.map(opt => (
                                                <Option key={String(opt.key)} text={opt.text} value={String(opt.key)}>{opt.text}</Option>
                                            ))}
                                        </Dropdown>
                                    </div>
                                    <div style={validateMapStackStyle}>
                                        <Button id="validateMap" onClick={validateActiveMap}>{nlsHPCC.Validate}</Button>
                                    </div>
                                </div>
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
                    </div>
                }
                {tab === "packageContents" &&
                    <div style={pivotItemStyle(size, 0)}>
                        <HolyGrail
                            header={
                                <div style={validateMapStackStyle}>
                                    <div style={validateMapStackStyle}>
                                        <Label>{nlsHPCC.Target}</Label>
                                        <Dropdown
                                            id="contentsTarget" className={validateMapStyles.dropdown}
                                            selectedOptions={contentsTarget ? [contentsTarget] : []}
                                            placeholder={nlsHPCC.SelectEllipsis}
                                            onOptionSelect={changeContentsTarget}
                                        >
                                            {targets?.map(opt => (
                                                <Option key={String(opt.key)} text={opt.text} value={String(opt.key)}>{opt.text}</Option>
                                            ))}
                                        </Dropdown>
                                    </div>
                                    <div style={validateMapStackStyle}>
                                        <Label>{nlsHPCC.Process}</Label>
                                        <Dropdown
                                            id="contentsProcess" className={validateMapStyles.dropdown}
                                            selectedOptions={contentsProcess ? [contentsProcess] : []}
                                            placeholder={nlsHPCC.SelectEllipsis}
                                            onOptionSelect={changeContentsProcess}
                                        >
                                            {processes?.map(opt => (
                                                <Option key={String(opt.key)} text={opt.text} value={String(opt.key)}>{opt.text}</Option>
                                            ))}
                                        </Dropdown>
                                    </div>
                                    <div style={validateMapStackStyle}>
                                        <input id="uploadMapFromFile" type="file" className={validateMapStyles.displayNone} accept="*.xml" onChange={handleFileSelect} />
                                        <Button id="loadMapFromFile" onClick={handleLoadMapFromFileClick}>{nlsHPCC.LoadPackageFromFile}</Button>
                                        <Button id="validateMap" onClick={validateContents}>{nlsHPCC.Validate}</Button>
                                    </div>
                                </div>
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
                    </div>
                }
            </div>
        }</SizeMe>
        <AddPackageMap
            showForm={showAddForm} setShowForm={setShowAddForm}
            refreshData={refreshData} targets={targets} processes={processes}
        />
        <DeleteConfirm />
    </>;
};