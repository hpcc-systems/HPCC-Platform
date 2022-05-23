import * as React from "react";
import { Checkbox, ChoiceGroup, IChoiceGroupOption, Dropdown as DropdownBase, IDropdownOption, TextField, Link, ProgressIndicator } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { TpDropZoneQuery, TpGroupQuery, TpServiceQuery } from "src/WsTopology";
import * as WsAccess from "src/ws_access";
import * as WsESDLConfig from "src/WsESDLConfig";
import { States } from "src/WsWorkunits";
import { FileList, States as DFUStates } from "src/FileSpray";
import { joinPath } from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useLogicalClusters } from "../../hooks/platform";

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
    errorMessage?: string;
    onChange?: (event: React.FormEvent<HTMLDivElement>, option?: IDropdownOption, index?: number) => void;
    placeholder?: string;
    className?: string;
}

const AsyncDropdown: React.FunctionComponent<AsyncDropdownProps> = ({
    label,
    options,
    selectedKey,
    required = false,
    disabled,
    errorMessage,
    onChange,
    placeholder,
    className
}) => {

    const selOptions = React.useMemo<IDropdownOption[]>(() => {
        if (options !== undefined) {
            return !required ? [{ key: "", text: "" }, ...options] : options;
        }
        return [];
    }, [options, required]);

    const [selectedItem, setSelectedItem] = React.useState<IDropdownOption>();
    React.useEffect(() => {
        setSelectedItem(selOptions?.find(row => row.key === selectedKey) ?? selOptions[0]);
    }, [selectedKey, selOptions]);

    const controlledChange = React.useCallback((event: React.FormEvent<HTMLDivElement>, item: IDropdownOption, idx: number): void => {
        setSelectedItem(item);
        onChange(event, item, idx);
    }, [onChange]);

    return options === undefined ?
        <DropdownBase label={label} options={[]} placeholder={nlsHPCC.loadingMessage} disabled={true} /> :
        <DropdownBase label={label} options={selOptions} selectedKey={selectedItem?.key} onChange={controlledChange} placeholder={placeholder} disabled={disabled} required={required} errorMessage={errorMessage} className={className} />;
};

const autoSelectDropdown = (selectedKey?: string, required?: boolean) => selectedKey === undefined && !required;

export type FieldType = "string" | "password" | "number" | "checkbox" | "choicegroup" | "datetime" | "dropdown" | "link" | "links" | "progress" |
    "workunit-state" |
    "file-type" | "file-sortby" |
    "queries-priority" | "queries-suspend-state" | "queries-active-state" |
    "target-cluster" | "target-dropzone" | "target-server" | "target-group" |
    "target-dfuqueue" | "user-groups" | "group-members" | "permission-type" |
    "logicalfile-type" | "dfuworkunit-state" |
    "esdl-esp-processes" | "esdl-definitions";

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
    value?: string;
}

interface TargetGroupField extends BaseField {
    type: "target-group";
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

type Field = StringField | NumericField | CheckboxField | ChoiceGroupField | DateTimeField | DropdownField | LinkField | LinksField | ProgressField |
    WorkunitStateField |
    FileTypeField | FileSortByField |
    QueriesPriorityField | QueriesSuspendStateField | QueriesActiveStateField |
    TargetClusterField | TargetDropzoneField | TargetServerField | TargetGroupField |
    TargetDfuSprayQueueField | UserGroupsField | GroupMembersField | PermissionTypeField |
    LogicalFileType | DFUWorkunitStateField |
    EsdlEspProcessesField | EsdlDefinitionsField;

export type Fields = { [id: string]: Field };

export interface TargetClusterTextFieldProps extends Omit<AsyncDropdownProps, "options"> {
}

export const TargetClusterTextField: React.FunctionComponent<TargetClusterTextFieldProps> = (props) => {

    const [targetClusters, defaultCluster] = useLogicalClusters();
    const [options, setOptions] = React.useState<IDropdownOption[]>();
    const [defaultRow, setDefaultRow] = React.useState<IDropdownOption>();
    const { onChange, required, selectedKey } = { ...props };

    React.useEffect(() => {
        const options = targetClusters?.filter(row => {
            return !(row.Type === "roxie" && row.QueriesOnly === true);
        })?.map(row => {
            return {
                key: row.Name || "unknown",
                text: row.Name + (row.Name !== row.Type ? ` (${row.Type})` : ""),
                type: row.Type
            };
        }) || [];
        setOptions(options);

        if (autoSelectDropdown(selectedKey, required)) {
            const selectedItem = options.filter(row => row.key === defaultCluster?.Name)[0];
            if (selectedItem) {
                setDefaultRow(selectedItem);
                onChange(undefined, selectedItem);
            }
        }
    }, [targetClusters, defaultCluster, onChange, required, selectedKey]);

    return <AsyncDropdown {...props} selectedKey={props.selectedKey || defaultRow?.key as string} options={options} />;
};

export interface TargetDropzoneTextFieldProps extends Omit<AsyncDropdownProps, "options"> {
}

export const TargetDropzoneTextField: React.FunctionComponent<TargetDropzoneTextFieldProps> = (props) => {

    const [targetDropzones, setTargetDropzones] = React.useState<IDropdownOption[]>();

    React.useEffect(() => {
        TpDropZoneQuery({}).then(({ TpDropZoneQueryResponse }) => {
            setTargetDropzones(TpDropZoneQueryResponse?.TpDropZones?.TpDropZone?.map((row, idx) => {
                return {
                    key: row.Name,
                    text: row.Name,
                    path: row.Path
                };
            }) || []);
        }).catch(err => logger.error(err));
    }, []);

    return <AsyncDropdown {...props} options={targetDropzones} />;
};

export interface TargetServerTextFieldProps extends Omit<AsyncDropdownProps, "options"> {
    dropzone: string;
}

export const TargetServerTextField: React.FunctionComponent<TargetServerTextFieldProps> = (props) => {

    const [targetServers, setTargetServers] = React.useState<IDropdownOption[]>();

    React.useEffect(() => {
        TpDropZoneQuery({ Name: "" }).then(response => {
            const { TpDropZoneQueryResponse } = response;
            setTargetServers(TpDropZoneQueryResponse?.TpDropZones?.TpDropZone?.filter(row => row.Name === props.dropzone)[0]?.TpMachines?.TpMachine?.map(n => {
                return {
                    key: n.ConfigNetaddress,
                    text: n.Netaddress,
                    OS: n.OS
                };
            }) || []);
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
            setTargetGroups(TpGroupQueryResponse.TpGroups.TpGroup.map(n => {
                return {
                    key: n.Name,
                    text: n.Name + (n.Name !== n.Kind ? ` (${n.Kind})` : "")
                };
            }));
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

export interface TargetFolderTextFieldProps extends Omit<AsyncDropdownProps, "options"> {
    pathSepChar?: string;
    machineAddress?: string;
    machineDirectory?: string;
    machineOS?: number;
}

export const TargetFolderTextField: React.FunctionComponent<TargetFolderTextFieldProps> = (props) => {

    const [folders, setFolders] = React.useState<IDropdownOption[]>();
    const { pathSepChar, machineAddress, machineDirectory, machineOS } = { ...props };

    const fetchFolders = React.useCallback((pathSepChar: string, Netaddr: string, Path: string, OS: number, depth: number): Promise<IDropdownOption[]> => {
        depth = depth || 0;
        let retVal: IDropdownOption[] = [];
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
                FileList({
                    request: {
                        Netaddr: Netaddr,
                        Path: Path,
                        OS: OS
                    },
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
    }, [machineDirectory, props.required]);

    React.useEffect(() => {
        const _fetchFolders = async () => {
            const folders = await fetchFolders(pathSepChar, machineAddress, machineDirectory, machineOS, 0);
            setFolders(folders);
        };
        if (machineAddress && machineDirectory && machineOS) {
            _fetchFolders();
        }
    }, [pathSepChar, machineAddress, machineDirectory, machineOS, fetchFolders]);

    return <AsyncDropdown {...props} options={folders} />;
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

    const [users, setUsers] = React.useState<IDropdownOption[]>();

    React.useEffect(() => {
        const request = { groupname: props.groupname };
        WsAccess.GroupMemberEditInput({ request: request }).then(({ GroupMemberEditInputResponse }) => {
            const _users = GroupMemberEditInputResponse?.Users?.User
                .map(user => {
                    return {
                        key: user.username,
                        text: user.username
                    };
                }) || [];
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
                        type={field.type}
                        name={fieldID}
                        value={field.value}
                        placeholder={field.placeholder}
                        onChange={(evt, newValue) => onChange(fieldID, newValue)}
                        borderless={field.readonly && !field.multiline}
                        readOnly={field.readonly}
                        required={field.required}
                        multiline={field.multiline}
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
                        name={fieldID}
                        disabled={field.disabled("") ? true : false}
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
            case "datetime":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <input
                        key={fieldID}
                        type="datetime-local"
                        name={fieldID}
                        defaultValue={field.value}
                        onChange={ev => {
                            field.value = ev.target.value;
                        }}
                    />
                });
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
                        percentComplete={parseInt(field.value, 10)} />
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
                        onChange={(ev, row) => onChange(fieldID, row.key)}
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
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <TargetClusterTextField
                        key={fieldID}
                        selectedKey={field.value}
                        onChange={(ev, row) => onChange(fieldID, row.key)}
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
                        onChange={(ev, row) => {
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
                        onChange={(ev, row) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                        setSetDropzone={_ => setDropzone = _}
                    />
                });
                break;
            case "target-group":
                field.value = field.value !== undefined ? field.value : "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <TargetGroupTextField
                        key={fieldID}
                        required={field.required}
                        selectedKey={field.value}
                        onChange={(ev, row) => onChange(fieldID, row.key)}
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
                        onChange={(ev, row) => onChange(fieldID, row.key)}
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
                        onChange={(ev, row) => onChange(fieldID, row.key)}
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
                        onChange={(ev, row) => onChange(fieldID, row.key)}
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
                        onChange={(ev, row) => onChange(fieldID, row.key)}
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
                        onChange={(ev, row) => onChange(fieldID, row.key)}
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
                        onChange={(ev, row) => onChange(fieldID, row.key)}
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
        }
    }
    return retVal;
}
