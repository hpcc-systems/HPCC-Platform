import * as React from "react";
import { Checkbox, ChoiceGroup, ComboBox, IChoiceGroupOption, Dropdown as DropdownBase, IDropdownOption, TextField, Link, ProgressIndicator, IComboBoxOption, IComboBoxProps, IComboBox } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { FileSpray } from "@hpcc-js/comms";
import { TpDropZoneQuery, TpGroupQuery, TpServiceQuery } from "src/WsTopology";
import * as WsAccess from "src/ws_access";
import * as WsESDLConfig from "src/WsESDLConfig";
import { States } from "src/WsWorkunits";
import { FileList, States as DFUStates } from "src/FileSpray";
import { joinPath } from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useBuildInfo, useLogicalClusters } from "../../hooks/platform";
import { useContainerNames, usePodNames } from "../../hooks/cloud";

const logger = scopedLogger("src-react/components/forms/Fields.tsx");

interface DropdownProps {
    label?: string;
    options?: IDropdownOption[];
    selectedKey?: string;
    defaultSelectedKey?: string;
    required?: boolean;
    optional?: boolean;
    disabled?: boolean;
    errorMessage?: string;
    onChange?: (event: React.FormEvent<HTMLDivElement>, option?: IDropdownOption, index?: number) => void;
    placeholder?: string;
    className?: string
}

const Dropdown: React.FunctionComponent<DropdownProps> = ({
    label,
    options = [],
    selectedKey,
    defaultSelectedKey,
    required = false,
    optional = !required,
    disabled,
    errorMessage,
    onChange,
    placeholder,
    className
}) => {
    React.useEffect(() => {
        if (required === true && optional === true) {
            logger.error(`${label}:  required == true and optional == true is illogical`);
        }
        if (defaultSelectedKey && selectedKey) {
            logger.error(`${label}:  Dropdown property 'defaultSelectedKey' is mutually exclusive with 'selectedKey' (${defaultSelectedKey}, ${selectedKey}). Use one or the other.`);
        }
    }, [defaultSelectedKey, label, optional, required, selectedKey]);

    const [selOptions, selKey] = React.useMemo(() => {
        const selOpts = optional ? [{ key: "", text: "" }, ...options] : [...options];
        if (options.length === 0) {
            return [selOpts, selectedKey];
        }

        let selRow;
        let selIdx;
        selOpts.forEach((row, idx) => {
            if (idx === 0 || row.key === selectedKey) {
                selRow = row;
                selIdx = idx;
            }
        });
        if (selRow && selRow.key !== selectedKey) {
            setTimeout(() => {
                onChange(undefined, selRow, selIdx);
            }, 1);
        }
        return [selOpts, selRow?.key];
    }, [onChange, optional, options, selectedKey]);

    return <DropdownBase label={label} errorMessage={errorMessage} required={required} selectedKey={selKey} defaultSelectedKey={defaultSelectedKey} onChange={onChange} placeholder={placeholder} options={selOptions} disabled={disabled} className={className} />;
};

interface AsyncDropdownProps {
    label?: string;
    options?: IDropdownOption[];
    selectedKey?: string;
    required?: boolean;
    disabled?: boolean;
    multiSelect?: boolean;
    valueSeparator?: string;
    errorMessage?: string;
    onChange?: (event: React.FormEvent<HTMLDivElement>, option?: IDropdownOption | IDropdownOption[], index?: number) => void;
    placeholder?: string;
    className?: string;
}

const AsyncDropdown: React.FunctionComponent<AsyncDropdownProps> = ({
    label,
    options,
    selectedKey,
    required = false,
    disabled,
    multiSelect = false,
    valueSeparator = "|",
    errorMessage,
    onChange,
    placeholder,
    className
}) => {

    const selOptions = React.useMemo<IDropdownOption[]>(() => {
        if (options !== undefined) {
            return !required && !multiSelect ? [{ key: "", text: "" }, ...options] : options;
        }
        return [];
    }, [multiSelect, options, required]);
    const [selectedItem, setSelectedItem] = React.useState<IDropdownOption>();
    const [selectedIdx, setSelectedIdx] = React.useState<number>();

    const [selectedKeys, setSelectedKeys] = React.useState(selectedKey ?? "");
    const [selectedItems, setSelectedItems] = React.useState<IDropdownOption[]>([]);

    const changeSelectedItems = React.useCallback(() => {
        const keys = selectedKey !== "" ? selectedKey.split(valueSeparator) : [];
        let items = [...selectedItems];
        if (keys.length === items.length) return;
        if (selectedKeys !== "" && selOptions.length && selectedKey === "" && selectedKeys === items.map(i => i.key).join("|")) {
            setSelectedItems([]);
            return;
        }
        items = keys.map(key => { return { key: key, text: key }; });
        if (!items.length) return;
        if (items.map(item => item.key).join(valueSeparator) === selectedKey) {
            // do nothing, unless
            if (!selectedItems.length) {
                setSelectedItems(items);
            }
        } else {
            setSelectedKeys(items.map(item => item.key).join(valueSeparator));
            setSelectedItems(items);
        }
    }, [selectedKey, selectedKeys, selectedItems, selOptions, valueSeparator]);

    React.useEffect(() => {
        // only on mount, pre-populate selectedItems from url
        if (multiSelect) {
            changeSelectedItems();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    React.useEffect(() => {
        if (multiSelect) {
            if (!selectedItems.length) return;
            changeSelectedItems();
        } else {
            let item;
            if (selectedItem?.key) {
                item = selOptions?.find(row => row.key === selectedItem?.key) ?? selOptions[0];
            } else {
                item = selOptions?.find(row => row.key === selectedKey) ?? selOptions[0];
            }
            if (!item) return;
            if (item.key === selectedKey) {
                // do nothing, unless
                if (!selectedItem) {
                    setSelectedItem(item);
                    setSelectedIdx(selOptions.indexOf(item));
                }
            } else {
                setSelectedItem(item);
                setSelectedIdx(selOptions.indexOf(item));
            }
        }
    }, [changeSelectedItems, multiSelect, selectedKey, selOptions, selectedItem, selectedItems]);

    React.useEffect(() => {
        if (multiSelect) {
            if (!selectedItems.length && selectedKey === "") return;
            if (selectedItems.map(item => item.key).join(valueSeparator) === selectedKey) return;
            onChange(undefined, selectedItems, null);
        } else {
            if (!selectedItem || selectedItem?.key === selectedKey) return;
            if (selectedItem !== undefined) {
                onChange(undefined, selectedItem, selectedIdx);
            }
        }
    }, [onChange, multiSelect, selectedItem, selectedIdx, selectedKey, selectedItems, valueSeparator]);

    if (multiSelect) {
        return options === undefined ?
            <DropdownBase label={label} multiSelect dropdownWidth="auto" options={[]} placeholder={nlsHPCC.loadingMessage} disabled={true} /> :
            <DropdownBase label={label} multiSelect dropdownWidth="auto" options={selOptions} selectedKeys={selectedItems.map(item => item.key as string)} onChange={
                (_, item: IDropdownOption) => {
                    if (item) {
                        let selected = selectedItems.filter(i => i.key !== item.key);
                        if (item.selected) {
                            selected = [...selectedItems, item];
                        }
                        setSelectedItems(selected);
                    }
                }
            } placeholder={placeholder} disabled={disabled} required={required} errorMessage={errorMessage} className={className} />;
    } else {
        return options === undefined ?
            <DropdownBase label={label} dropdownWidth="auto" options={[]} placeholder={nlsHPCC.loadingMessage} disabled={true} /> :
            <DropdownBase label={label} dropdownWidth="auto" options={selOptions} selectedKey={selectedItem?.key} onChange={(_, item: IDropdownOption) => setSelectedItem(item)} placeholder={placeholder} disabled={disabled} required={required} errorMessage={errorMessage} className={className} />;
    }
};

interface DropdownMultiProps {
    label?: string;
    options?: IDropdownOption[];
    selectedKeys?: string;
    defaultSelectedKeys?: string;
    required?: boolean;
    optional?: boolean;
    disabled?: boolean;
    errorMessage?: string;
    onChange?: (event: React.FormEvent<HTMLDivElement>, value?: string) => void;
    placeholder?: string;
    className?: string
}

const DropdownMulti: React.FunctionComponent<DropdownMultiProps> = ({
    label,
    options = [],
    selectedKeys,
    defaultSelectedKeys,
    required = false,
    optional = !required,
    disabled,
    errorMessage,
    onChange,
    placeholder,
    className
}) => {
    const defaultSelKeys = React.useMemo(() => {
        if (defaultSelectedKeys) {
            return defaultSelectedKeys.split(",");
        }
        return [];
    }, [defaultSelectedKeys]);

    const selKeys = React.useMemo(() => {
        if (selectedKeys) {
            return selectedKeys.split(",");
        }
        return [];
    }, [selectedKeys]);

    const localOnChange = React.useCallback((event: React.FormEvent<HTMLDivElement>, item: IDropdownOption): void => {
        if (item) {
            const selected = item.selected ? [...selKeys, item.key as string] : selKeys.filter(key => key !== item.key);
            onChange(event, selected.join(","));
        }
    }, [onChange, selKeys]);

    return <DropdownBase label={label} errorMessage={errorMessage} required={required} multiSelect selectedKeys={selKeys} defaultSelectedKeys={defaultSelKeys} onChange={localOnChange} placeholder={placeholder} options={options} disabled={disabled} className={className} />;
};

export type FieldType = "string" | "password" | "number" | "checkbox" | "choicegroup" | "datetime" | "dropdown" | "dropdown-multi" |
    "link" | "links" | "progress" |
    "workunit-state" |
    "file-type" | "file-sortby" |
    "queries-priority" | "queries-suspend-state" | "queries-active-state" |
    "target-cluster" | "target-dropzone" | "target-server" | "target-group" |
    "target-dfuqueue" | "user-groups" | "group-members" | "permission-type" |
    "logicalfile-type" | "dfuworkunit-state" |
    "esdl-esp-processes" | "esdl-definitions" |
    "cloud-containername" | "cloud-podname";

export type Values = { [name: string]: string | number | boolean | (string | number | boolean)[] };

interface BaseField {
    type: FieldType;
    label: string;
    disabled?: (params) => boolean;
    placeholder?: string;
    readonly?: boolean;
    required?: boolean;
}

interface StringField extends BaseField {
    type: "string" | "password";
    value?: string;
    readonly?: boolean;
    multiline?: boolean;
    errorMessage?: string;
}

interface NumericField extends BaseField {
    type: "number";
    value?: number;
}

interface DateTimeField extends BaseField {
    type: "datetime";
    value?: string;
}

interface CheckboxField extends BaseField {
    type: "checkbox";
    value?: boolean;
}

interface ChoiceGroupField extends BaseField {
    type: "choicegroup";
    value?: string;
    options: IChoiceGroupOption[];
}

interface DropdownField extends BaseField {
    type: "dropdown";
    value?: string;
    options: IDropdownOption[];
}

interface DropdownMultiField extends BaseField {
    type: "dropdown-multi";
    value?: string;
    options: IDropdownOption[];
}

interface WorkunitStateField extends BaseField {
    type: "workunit-state";
    value?: string;
}

interface FileTypeField extends BaseField {
    type: "file-type";
    value?: string;
}

interface FileSortByField extends BaseField {
    type: "file-sortby";
    value?: string;
}

interface QueriesPriorityField extends BaseField {
    type: "queries-priority";
    value?: string;
}

interface QueriesSuspendStateField extends BaseField {
    type: "queries-suspend-state";
    value?: string;
}

interface QueriesActiveStateField extends BaseField {
    type: "queries-active-state";
    value?: string;
}

interface TargetClusterField extends BaseField {
    type: "target-cluster";
    multiSelect?: boolean;
    valueSeparator?: string;
    value?: string;
}

interface TargetGroupField extends BaseField {
    type: "target-group";
    multiSelect?: boolean;
    valueSeparator?: string;
    value?: string;
}

interface TargetServerField extends BaseField {
    type: "target-server";
    value?: string;
}

interface TargetDropzoneField extends BaseField {
    type: "target-dropzone";
    value?: string;
}

interface TargetDfuSprayQueueField extends BaseField {
    type: "target-dfuqueue";
    value?: string;
}

interface LogicalFileType extends BaseField {
    type: "logicalfile-type";
    value?: string;
}

interface DFUWorkunitStateField extends BaseField {
    type: "dfuworkunit-state";
    value?: string;
}

interface UserGroupsField extends BaseField {
    type: "user-groups";
    username: string;
    value?: string;
}

interface GroupMembersField extends BaseField {
    type: "group-members";
    groupname: string;
    value?: string;
}

interface PermissionTypeField extends BaseField {
    type: "permission-type";
    value?: string;
}

interface EsdlEspProcessesField extends BaseField {
    type: "esdl-esp-processes";
    value?: string;
}

interface EsdlDefinitionsField extends BaseField {
    type: "esdl-definitions";
    value?: string;
}

interface LinkField extends BaseField {
    type: "link";
    href: string;
    value?: string;
    newTab?: boolean;
}

interface LinksField extends BaseField {
    type: "links";
    links: LinkField[]
    value?: string;
}

interface ProgressField extends BaseField {
    type: "progress";
    value?: string;
}

interface CloudContainerNameField extends BaseField {
    type: "cloud-containername";
    value?: string;
}

interface CloudPodNameField extends BaseField {
    type: "cloud-podname";
    value?: string;
}

export type Field = StringField | NumericField | CheckboxField | ChoiceGroupField | DateTimeField | DropdownField | DropdownMultiField |
    LinkField | LinksField | ProgressField |
    WorkunitStateField |
    FileTypeField | FileSortByField |
    QueriesPriorityField | QueriesSuspendStateField | QueriesActiveStateField |
    TargetClusterField | TargetDropzoneField | TargetServerField | TargetGroupField |
    TargetDfuSprayQueueField | UserGroupsField | GroupMembersField | PermissionTypeField |
    LogicalFileType | DFUWorkunitStateField |
    EsdlEspProcessesField | EsdlDefinitionsField |
    CloudContainerNameField | CloudPodNameField;

export type Fields = { [id: string]: Field };

export interface TargetClusterTextFieldProps extends Omit<AsyncDropdownProps, "options"> {
    excludeRoxie?: boolean;
}

export interface TargetClusterOption extends IDropdownOption {
    queriesOnly: boolean;
}

export const TargetClusterTextField: React.FunctionComponent<TargetClusterTextFieldProps> = (props) => {

    const [targetClusters, defaultCluster] = useLogicalClusters();
    const [options, setOptions] = React.useState<IDropdownOption[] | undefined>();
    const { excludeRoxie = true } = { ...props };

    React.useEffect(() => {
        let clusters = targetClusters;
        if (excludeRoxie) {
            clusters = clusters?.filter(row => {
                return !(row.Type === "roxie" && row.QueriesOnly === true);
            });
        }
        setOptions(clusters?.map(row => {
            return {
                key: row.Name || "unknown",
                text: row.Name + (row.Name !== row.Type ? ` (${row.Type})` : ""),
                selected: row.Name === defaultCluster?.Name,
                type: row.Type,
                queriesOnly: row.QueriesOnly || false
            };
        }));
    }, [defaultCluster, excludeRoxie, targetClusters]);

    return <AsyncDropdown {...props} options={options} />;
};

export interface TargetDropzoneTextFieldProps extends Omit<AsyncDropdownProps, "options"> {
}

export const TargetDropzoneTextField: React.FunctionComponent<TargetDropzoneTextFieldProps> = (props) => {

    const [targetDropzones, setTargetDropzones] = React.useState<IDropdownOption[] | undefined>();

    React.useEffect(() => {
        TpDropZoneQuery({}).then(({ TpDropZoneQueryResponse }) => {
            setTargetDropzones(TpDropZoneQueryResponse?.TpDropZones?.TpDropZone?.map((row, idx) => {
                return {
                    key: row.Name,
                    text: row.Name,
                    path: row.Path,
                    OS: row?.TpMachines?.TpMachine[0].OS ?? ""
                };
            }));
        }).catch(err => logger.error(err));
    }, []);

    return <AsyncDropdown {...props} options={targetDropzones} />;
};

export interface TargetServerTextFieldProps extends Omit<AsyncDropdownProps, "options"> {
    dropzone: string;
}

export const TargetServerTextField: React.FunctionComponent<TargetServerTextFieldProps> = (props) => {

    const [targetServers, setTargetServers] = React.useState<IDropdownOption[] | undefined>();

    React.useEffect(() => {
        TpDropZoneQuery({ request: { Name: "" } }).then(response => {
            const { TpDropZoneQueryResponse } = response;
            setTargetServers(TpDropZoneQueryResponse?.TpDropZones?.TpDropZone?.filter(row => row.Name === props.dropzone)[0]?.TpMachines?.TpMachine?.map(n => {
                return {
                    key: n.Netaddress,
                    text: n.Netaddress,
                    OS: n.OS
                };
            }));
        }).catch(err => logger.error(err));
    }, [props.selectedKey, props.dropzone]);

    return <AsyncDropdown {...props} options={targetServers} />;
};

export interface TargetServerTextFieldLinkedProps extends Omit<AsyncDropdownProps, "options"> {
    setSetDropzone?: (setDropzone: (dropzone: string) => void) => void;
}

export const TargetServerTextLinkedField: React.FunctionComponent<TargetServerTextFieldLinkedProps> = (props) => {

    const [dropzone, setDropzone] = React.useState("");

    React.useEffect(() => {
        if (props.setSetDropzone) {
            props.setSetDropzone(setDropzone);
        }
    }, [props, props.setSetDropzone]);

    return <TargetServerTextField {...props} dropzone={dropzone} />;
};

export interface TargetGroupTextFieldProps extends Omit<AsyncDropdownProps, "options"> {
}

export const TargetGroupTextField: React.FunctionComponent<TargetGroupTextFieldProps> = (props) => {

    const [targetGroups, setTargetGroups] = React.useState<IDropdownOption[]>();

    React.useEffect(() => {
        TpGroupQuery({}).then(({ TpGroupQueryResponse }) => {
            const groups = TpGroupQueryResponse?.TpGroups?.TpGroup ?? [];
            setTargetGroups(groups.map(group => {
                switch (group?.Kind) {
                    case "Thor":
                    case "hthor":
                    case "Roxie":
                    case "Plane":
                        return {
                            key: group.Name,
                            text: group.Name + (group.Name !== group.Kind ? ` (${group.Kind})` : "")
                        };
                }
            }).filter(group => group));
        }).catch(err => logger.error(err));
    }, []);

    return <AsyncDropdown {...props} options={targetGroups} />;
};

export interface TargetDfuSprayQueueTextFieldProps extends Omit<AsyncDropdownProps, "options"> {
}

export const TargetDfuSprayQueueTextField: React.FunctionComponent<TargetDfuSprayQueueTextFieldProps> = (props) => {

    const [dfuSprayQueues, setDfuSprayQueues] = React.useState<IDropdownOption[]>();

    React.useEffect(() => {
        TpServiceQuery({}).then(({ TpServiceQueryResponse }) => {
            setDfuSprayQueues(
                TpServiceQueryResponse.ServiceList.TpDfuServers.TpDfuServer.map(n => {
                    return {
                        key: n.Queue,
                        text: n.Queue
                    };
                })
            );
        }).catch(err => logger.error(err));
    }, []);

    return <AsyncDropdown {...props} options={dfuSprayQueues} />;
};

export interface EsdlEspProcessesTextFieldProps extends Omit<AsyncDropdownProps, "options"> {
}

export const EsdlEspProcessesTextField: React.FunctionComponent<EsdlEspProcessesTextFieldProps> = (props) => {

    const [espProcesses, setEspProcesses] = React.useState<IDropdownOption[]>([]);

    React.useEffect(() => {
        WsESDLConfig.ListESDLBindings({}).then(({ ListESDLBindingsResponse }) => {
            setEspProcesses(
                ListESDLBindingsResponse?.EspProcesses?.EspProcess?.map(proc => {
                    return {
                        key: proc.Name,
                        text: proc.Name
                    };
                }) ?? []
            );
        });
    }, []);

    return <AsyncDropdown {...props} options={espProcesses} />;
};
export interface EsdlDefinitionsTextFieldProps extends Omit<AsyncDropdownProps, "options"> {
}

export const EsdlDefinitionsTextField: React.FunctionComponent<EsdlDefinitionsTextFieldProps> = (props) => {

    const [definitions, setDefinitions] = React.useState<IDropdownOption[]>([]);

    React.useEffect(() => {
        WsESDLConfig.ListESDLDefinitions({}).then(({ ListESDLDefinitionsResponse }) => {
            setDefinitions(
                ListESDLDefinitionsResponse?.Definitions?.Definition?.map(defn => {
                    return {
                        key: defn.Id,
                        text: defn.Id
                    };
                }) ?? []
            );
        });
    }, []);

    return <AsyncDropdown {...props} options={definitions} />;
};

export interface TargetFolderTextFieldProps extends Omit<IComboBoxProps, "options"> {
    pathSepChar?: string;
    dropzone?: string;
    machineAddress?: string;
    machineDirectory?: string;
    machineOS?: number;
}

export const TargetFolderTextField: React.FunctionComponent<TargetFolderTextFieldProps> = (props) => {

    const [, { isContainer }] = useBuildInfo();
    const [folders, setFolders] = React.useState<IComboBoxOption[]>();
    const [selectedKey, setSelectedKey] = React.useState<string | number | undefined>();
    const { pathSepChar, dropzone, machineAddress, machineDirectory, machineOS, onChange } = { ...props };

    const styles = React.useMemo(() => {
        return { root: { width: 320 }, optionsContainerWrapper: { width: 320 } };
    }, []);

    const fetchFolders = React.useCallback((pathSepChar: string, Netaddr: string, Path: string, OS: number, depth: number): Promise<IComboBoxOption[]> => {
        depth = depth || 0;
        let retVal: IComboBoxOption[] = [];
        if (!props.required) {
            retVal.push({ key: "", text: "" });
        }
        retVal.push({
            key: Path,
            text: joinPath(Path, pathSepChar).replace(machineDirectory, "")
        });
        return new Promise((resolve, reject) => {
            if (depth > 2) {
                resolve(retVal);
            } else {
                const request: Partial<FileSpray.FileListRequest> = {
                    Path: Path,
                    OS: OS?.toString() ?? ""
                };
                if (isContainer) {
                    request.DropZoneName = dropzone;
                } else {
                    request.Netaddr = Netaddr;
                }
                FileList({
                    request,
                    suppressExceptionToaster: true
                }).then(({ FileListResponse }) => {
                    const requests = [];
                    FileListResponse.files?.PhysicalFileStruct?.forEach(file => {
                        if (file.isDir) {
                            if (Path + pathSepChar === "//") {
                                requests.push(fetchFolders(pathSepChar, Netaddr, Path + file.name, OS, ++depth));
                            } else {
                                requests.push(fetchFolders(pathSepChar, Netaddr, [Path, file.name].join(pathSepChar), OS, ++depth));
                            }
                        }
                    });
                    Promise.all(requests).then(responses => {
                        responses.forEach(response => {
                            retVal = retVal.concat(response);
                        });
                        resolve(retVal);
                    }).catch(err => logger.error(err));
                });
            }
        });
    }, [dropzone, isContainer, machineDirectory, props.required]);

    const onChanged = React.useCallback((event: React.FormEvent<IComboBox>, option?: IComboBoxOption, index?: number, value?: string): void => {
        let key = option?.key;
        if (!option && value) {
            key = [machineDirectory, value].join(pathSepChar);
            setFolders(prevOptions => [...prevOptions, { key: key!, text: value }]);
        }
        setSelectedKey(key);
        onChange(event, option, index, value);
    }, [machineDirectory, onChange, pathSepChar]);

    React.useEffect(() => {
        if ((!isContainer && !machineAddress) || !machineDirectory || !dropzone || !machineOS) return;
        const _fetchFolders = async () => {
            const folders = await fetchFolders(pathSepChar, machineAddress, machineDirectory, machineOS, 0);
            setFolders(folders.sort((a, b) => {
                if (a.text < b.text) return -1;
                if (a.text > b.text) return 1;
                return 0;
            }));
        };
        _fetchFolders();
    }, [isContainer, pathSepChar, dropzone, machineAddress, machineDirectory, machineOS, fetchFolders]);

    return <ComboBox {...props} allowFreeform={true} autoComplete={"on"} selectedKey={selectedKey} onChange={onChanged} options={folders} styles={styles} />;
};

export interface UserGroupsProps extends Omit<AsyncDropdownProps, "options"> {
    username: string;
}

export const UserGroupsField: React.FunctionComponent<UserGroupsProps> = (props) => {

    const [groups, setGroups] = React.useState<IDropdownOption[]>();

    React.useEffect(() => {
        WsAccess.UserGroupEditInput({ request: { username: props.username } })
            .then(({ UserGroupEditInputResponse }) => {
                const groups = UserGroupEditInputResponse?.Groups?.Group
                    .filter(group => group.name !== "Administrators")
                    .sort((l, r) => l.name.localeCompare(r.name))
                    .map(group => {
                        return {
                            key: group.name,
                            text: group.name
                        };
                    }) || [];
                setGroups(groups || []);
            }).catch(err => logger.error(err));
    }, [props.username]);

    return <AsyncDropdown {...props} options={groups} />;
};

export interface GroupMembersProps extends Omit<AsyncDropdownProps, "options"> {
    groupname: string;
}

export const GroupMembersField: React.FunctionComponent<GroupMembersProps> = (props) => {

    const [users, setUsers] = React.useState<IDropdownOption[]>([]);

    React.useEffect(() => {
        const request = { groupname: props.groupname };
        WsAccess.GroupMemberEditInput({ request: request }).then(({ GroupMemberEditInputResponse }) => {
            const usersArray = GroupMemberEditInputResponse?.Users?.User || [];

            const _users = usersArray.map(user => ({
                key: user.username,
                text: user.username
            })).sort((a, b) => a.text.localeCompare(b.text));

            setUsers(_users);
        }).catch(err => logger.error(err));
    }, [props.groupname]);

    return <AsyncDropdown {...props} options={users} />;
};

export interface PermissionTypeProps extends Omit<AsyncDropdownProps, "options"> {
}

export const PermissionTypeField: React.FunctionComponent<PermissionTypeProps> = (props) => {

    const [baseDns, setBaseDns] = React.useState<IDropdownOption[]>();

    React.useEffect(() => {
        WsAccess.Permissions({}).then(({ BasednsResponse }) => {
            const _basedns = BasednsResponse?.Basedns?.Basedn
                .map(dn => {
                    return {
                        key: dn.name,
                        text: dn.name
                    };
                }) || [];
            setBaseDns(_basedns);
        }).catch(err => logger.error(err));
    }, []);

    return <AsyncDropdown {...props} options={baseDns} />;
};

export interface CloudContainerNameFieldProps extends Omit<IComboBoxProps, "options"> {
    name?: string;
}

export const CloudContainerNameField: React.FunctionComponent<CloudContainerNameFieldProps> = (props) => {

    const [cloudContainerNames] = useContainerNames();
    const [options, setOptions] = React.useState<IComboBoxOption[]>();

    React.useEffect(() => {
        const options = cloudContainerNames?.map(row => {
            return {
                key: row,
                text: row
            };
        }) || [];
        setOptions([{ key: "", text: "" }, ...options]);
    }, [cloudContainerNames]);

    return <ComboBox {...props} allowFreeform={true} autoComplete={"on"} options={options} />;
};

export interface CloudPodNameFieldProps extends Omit<IComboBoxProps, "options"> {
    name?: string;
}

export const CloudPodNameField: React.FunctionComponent<CloudPodNameFieldProps> = (props) => {

    const [cloudPodNames] = usePodNames();
    const [options, setOptions] = React.useState<IComboBoxOption[]>();

    React.useEffect(() => {
        const options = cloudPodNames?.map(row => {
            return {
                key: row,
                text: row
            };
        }) || [];
        setOptions(options);
    }, [cloudPodNames]);

    return <ComboBox {...props} allowFreeform={true} autoComplete={"on"} options={options} />;
};

const states = Object.keys(States).map(s => States[s]);
const dfustates = Object.keys(DFUStates).map(s => DFUStates[s]);

export function createInputs(fields: Fields, onChange?: (id: string, newValue: any) => void) {
    const retVal: { id: string, label: string, field: any }[] = [];
    let setDropzone = (dropzone: string) => { };
    for (const fieldID in fields) {
        const field = fields[fieldID];
        if (!field.disabled) {
            field.disabled = () => false;
        }
        switch (field.type) {
            case "string":
            case "password":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <TextField
                        key={fieldID}
                        id={fieldID}
                        type={field.type}
                        name={fieldID}
                        value={field.value}
                        title={field.value}
                        placeholder={field.placeholder}
                        onChange={(evt, newValue) => onChange(fieldID, newValue)}
                        borderless={field.readonly && !field.multiline}
                        readOnly={field.readonly}
                        disabled={field.disabled(field) ? true : false}
                        required={field.required}
                        multiline={field.multiline}
                        errorMessage={field.errorMessage ?? ""}
                        canRevealPassword={field.type === "password" ? true : false}
                        revealPasswordAriaLabel={nlsHPCC.ShowPassword}
                    />
                });
                break;
            case "number":
                field.value = field.value !== undefined ? field.value : 0;
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <TextField
                        key={fieldID}
                        id={fieldID}
                        type={field.type}
                        name={fieldID}
                        value={`${field.value}`}
                        placeholder={field.placeholder}
                        onChange={(evt, newValue) => onChange(fieldID, newValue)}
                        borderless={field.readonly}
                        readOnly={field.readonly}
                        required={field.required}
                    />
                });
                break;
            case "checkbox":
                field.value = field.value || false;
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <Checkbox
                        key={fieldID}
                        id={fieldID}
                        name={fieldID}
                        disabled={field.disabled(fields) ? true : false}
                        checked={field.value === true ? true : false}
                        onChange={(evt, newValue) => onChange(fieldID, newValue)}
                    />
                });
                break;
            case "choicegroup":
                field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <ChoiceGroup
                        key={fieldID}
                        name={fieldID}
                        disabled={field.disabled("") ? true : false}
                        selectedKey={field.value}
                        options={field.options ? field.options : []}
                        onChange={(evt, newValue) => onChange(fieldID, newValue)}
                    />
                });
                break;
            case "dropdown":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        selectedKey={field.value}
                        options={field.options}
                        onChange={(ev, row) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "dropdown-multi":
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <DropdownMulti
                        key={fieldID}
                        selectedKeys={field.value}
                        options={field.options}
                        onChange={(ev, value) => onChange(fieldID, value)}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "datetime":
                let dateStr;
                // the html input "datetime-local" expects the value to be of the format "YYYY-MM-DDTHH:mm"
                if (typeof field.value === "object") {
                    // but comes from the Logs component's filter initally as a Date
                    dateStr = field.value ? new Date(field.value).toISOString() : "";
                    field.value = dateStr.substring(0, dateStr.lastIndexOf(":"));
                } else {
                    // if not a complete ISO string, the datetime-local will creep forward
                    // in time with every subsequent new Date() (opening & closing
                    // the filter multiple times, for example)
                    if (field.value && field.value.indexOf("Z") < 0) {
                        field.value += ":00.000Z";
                    }
                    dateStr = field.value ? new Date(field.value).toISOString() : "";
                    field.value = dateStr.substring(0, dateStr.lastIndexOf(":"));
                }
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <input
                        key={fieldID}
                        id={fieldID}
                        type="datetime-local"
                        name={fieldID}
                        defaultValue={field.value}
                        onChange={ev => {
                            field.value = ev.target.value;
                        }}
                    />
                });
                const el = document.querySelector(`.ms-Modal.is-open #${fieldID}`);
                if (el && field.value === "") {
                    el["value"] = field.value;
                }
                break;
            case "link":
                field.href = field.href;
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <Link
                        key={fieldID}
                        href={field.href}
                        target={field.newTab ? "_blank" : ""}
                        style={{ paddingLeft: 8 }}>{field.value || ""}</Link>
                });
                break;
            case "links":
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: field.links?.map((link, idx) => <Link
                        key={`${fieldID}_${idx}`}
                        href={link.href}
                        target={link.newTab ? "_blank" : ""}
                        style={{ paddingLeft: 8 }}>{link.value || ""}</Link>
                    )
                });
                break;
            case "progress":
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <ProgressIndicator
                        key={fieldID}
                        percentComplete={parseInt(field.value, 10) / 100} />
                });
                break;
            case "workunit-state":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        selectedKey={field.value}
                        optional
                        options={states.map(state => {
                            return {
                                key: state,
                                text: state
                            };
                        })}
                        onChange={(ev, row) => {
                            onChange(fieldID, row.key);
                        }}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "file-type":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        selectedKey={field.value}
                        options={[
                            { key: "", text: nlsHPCC.LogicalFilesAndSuperfiles },
                            { key: "Logical Files Only", text: nlsHPCC.LogicalFilesOnly },
                            { key: "Superfiles Only", text: nlsHPCC.SuperfilesOnly },
                            { key: "Not in Superfiles", text: nlsHPCC.NotInSuperfiles },
                        ]}
                        onChange={(ev, row) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "file-sortby":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        selectedKey={field.value}
                        optional
                        options={[
                            { key: "Newest", text: nlsHPCC.Newest },
                            { key: "Oldest", text: nlsHPCC.Oldest },
                            { key: "Smallest", text: nlsHPCC.Smallest },
                            { key: "Largest", text: nlsHPCC.Largest }
                        ]}
                        onChange={(ev, row) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "queries-priority":
                field.value = field.value === undefined ? "" : field.value;
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        selectedKey={field.value.toString()}
                        options={[
                            { key: "", text: nlsHPCC.None },
                            { key: "0", text: nlsHPCC.Low },
                            { key: "1", text: nlsHPCC.High },
                            { key: "2", text: nlsHPCC.SLA }
                        ]}
                        onChange={(ev, row) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "queries-suspend-state":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        selectedKey={field.value}
                        optional
                        options={[
                            { key: "Not suspended", text: nlsHPCC.NotSuspended },
                            { key: "Suspended", text: nlsHPCC.Suspended },
                            { key: "Suspended by user", text: nlsHPCC.SuspendedByUser },
                            { key: "Suspended by first node", text: nlsHPCC.SuspendedByFirstNode },
                            { key: "Suspended by any node", text: nlsHPCC.SuspendedByAnyNode },
                        ]}
                        onChange={(ev, row) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "queries-active-state":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        selectedKey={field.value}
                        optional
                        options={[
                            { key: "1", text: nlsHPCC.Active },
                            { key: "0", text: nlsHPCC.NotActive }
                        ]}
                        onChange={(ev, row) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "target-cluster":
                field.value = field.value !== undefined ? field.value : "";
                field.valueSeparator = field.valueSeparator !== undefined ? field.valueSeparator : "|";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <TargetClusterTextField
                        key={fieldID}
                        multiSelect={field.multiSelect}
                        valueSeparator={field.valueSeparator}
                        selectedKey={field.value}
                        onChange={(ev, row) => {
                            if (field.multiSelect) {
                                onChange(fieldID, (row as IDropdownOption[]).map(i => i.key).join(field.valueSeparator));
                            } else {
                                onChange(fieldID, (row as IDropdownOption).key);
                            }
                        }}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "target-dropzone":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <TargetDropzoneTextField
                        key={fieldID}
                        selectedKey={field.value}
                        onChange={(ev, row: IDropdownOption) => {
                            onChange(fieldID, row.key);
                            setDropzone(row.key as string);
                        }}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "target-server":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <TargetServerTextLinkedField
                        key={fieldID}
                        selectedKey={field.value}
                        onChange={(ev, row: IDropdownOption) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                        setSetDropzone={_ => setDropzone = _}
                    />
                });
                break;
            case "target-group":
                field.value = field.value !== undefined ? field.value : "";
                field.valueSeparator = field.valueSeparator !== undefined ? field.valueSeparator : ",";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <TargetGroupTextField
                        key={fieldID}
                        required={field.required}
                        selectedKey={field.value}
                        multiSelect={field.multiSelect}
                        valueSeparator={field.valueSeparator}
                        onChange={(ev, row) => {
                            if (field.multiSelect) {
                                onChange(fieldID, (row as IDropdownOption[]).map(i => i.key).join(field.valueSeparator));
                            } else {
                                onChange(fieldID, (row as IDropdownOption).key);
                            }
                        }}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "user-groups":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <UserGroupsField
                        key={fieldID}
                        username={field.username}
                        required={field.required}
                        selectedKey={field.value}
                        onChange={(ev, row: IDropdownOption) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "group-members":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <GroupMembersField
                        key={fieldID}
                        groupname={field.groupname}
                        required={field.required}
                        selectedKey={field.value}
                        onChange={(ev, row: IDropdownOption) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "permission-type":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <PermissionTypeField
                        key={fieldID}
                        required={field.required}
                        selectedKey={field.value}
                        onChange={(ev, row: IDropdownOption) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "target-dfuqueue":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <TargetDfuSprayQueueTextField
                        key={fieldID}
                        selectedKey={field.value}
                        onChange={(ev, row: IDropdownOption) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "esdl-esp-processes":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <EsdlEspProcessesTextField
                        key={fieldID}
                        selectedKey={field.value}
                        onChange={(ev, row: IDropdownOption) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "esdl-definitions":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <EsdlDefinitionsTextField
                        key={fieldID}
                        selectedKey={field.value}
                        onChange={(ev, row: IDropdownOption) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "dfuworkunit-state":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        selectedKey={field.value}
                        optional
                        options={dfustates.map(state => {
                            return {
                                key: state,
                                text: state
                            };
                        })}
                        onChange={(ev, row) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "logicalfile-type":
                field.value = field.value !== undefined ? field.value : "Created";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        selectedKey={field.value}
                        optional
                        // disabled={field.disabled ? field.disabled(field) : false}
                        options={[
                            { key: "Created", text: nlsHPCC.CreatedByWorkunit },
                            { key: "Used", text: nlsHPCC.UsedByWorkunit }
                        ]}
                        onChange={(ev, row) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "cloud-containername":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <CloudContainerNameField
                        key={fieldID}
                        selectedKey={field.value}
                        onChange={(ev, row) => {
                            onChange(fieldID, row.key);
                            setDropzone(row.key as string);
                        }}
                        placeholder={field.placeholder}
                    />
                });
                break;
            case "cloud-podname":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <CloudPodNameField
                        key={fieldID}
                        onChange={(ev, row) => {
                            onChange(fieldID, row.key);
                            setDropzone(row.key as string);
                        }}
                        placeholder={field.placeholder}
                    />
                });
                break;
        }
    }
    return retVal;
}
