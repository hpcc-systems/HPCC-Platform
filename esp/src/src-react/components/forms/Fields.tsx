import * as React from "react";
import { Checkbox, ChoiceGroup, IChoiceGroupOption, Dropdown as DropdownBase, IDropdownOption, TextField, Link, ProgressIndicator } from "@fluentui/react";
import { TextField as MaterialUITextField } from "@material-ui/core";
import { Topology, TpLogicalClusterQuery } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { TpDropZoneQuery, TpGroupQuery, TpServiceQuery } from "src/WsTopology";
import * as WsAccess from "src/ws_access";
import { States } from "src/WsWorkunits";
import { FileList, States as DFUStates } from "src/FileSpray";
import nlsHPCC from "src/nlsHPCC";

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
    optional?: boolean;
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
    optional = !required,
    disabled,
    errorMessage,
    onChange,
    placeholder,
    className
}) => {

    let isOptional;
    let selOptions;
    let selKey = selectedKey;
    if (options !== undefined) {
        isOptional = isOptionalDropdown(required, optional);
        selOptions = isOptional ? [{ key: "", text: "" }, ...options] : options;
        if (!selOptions.some(row => row.key === selKey)) {
            selKey = selOptions[0]?.key;
            setTimeout(() => {
                onChange(undefined, selOptions[0], 0);
            }, 1);
        }
    }

    return options === undefined ?
        <DropdownBase label={label} options={[]} placeholder={nlsHPCC.loadingMessage} disabled={true} /> :
        <DropdownBase label={label} options={selOptions} selectedKey={selKey} onChange={onChange} placeholder={placeholder} disabled={disabled} required={required} errorMessage={errorMessage} className={className} />;
};

const isOptionalDropdown = (required?: boolean, optional?: boolean) => required === false || optional === true;
const autoSelectDropdown = (selectedKey?: string, required?: boolean, optional?: boolean) => selectedKey === undefined && isOptionalDropdown(required, optional);

export type FieldType = "string" | "password" | "number" | "checkbox" | "choicegroup" | "datetime" | "dropdown" | "link" | "links" | "progress" |
    "workunit-state" |
    "file-type" | "file-sortby" |
    "queries-priority" | "queries-suspend-state" | "queries-active-state" |
    "target-cluster" | "target-dropzone" | "target-server" | "target-group" |
    "target-dfuqueue" | "user-groups" | "group-members" | "permission-type" |
    "logicalfile-type" | "dfuworkunit-state";

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
    LogicalFileType | DFUWorkunitStateField;

export type Fields = { [id: string]: Field };

export interface TargetClusterTextFieldProps extends AsyncDropdownProps {
}

export const TargetClusterTextField: React.FunctionComponent<TargetClusterTextFieldProps> = (props) => {

    const [targetClusters, setTargetClusters] = React.useState<IDropdownOption[]>();
    const [defaultRow, setDefaultRow] = React.useState<IDropdownOption>();

    React.useEffect(() => {
        const topology = Topology.attach({ baseUrl: "" });
        let active = true;
        topology.fetchLogicalClusters().then((response: TpLogicalClusterQuery.TpLogicalCluster[]) => {
            if (active) {
                const options = response.map(row => {
                    return {
                        key: row.Name || "unknown",
                        text: row.Name + (row.Name !== row.Type ? ` (${row.Type})` : ""),
                        type: row.Type
                    };
                }) || [];
                setTargetClusters(options);
                let firstRow: IDropdownOption;
                let firstHThor: IDropdownOption;
                let firstThor: IDropdownOption;
                options.forEach(row => {
                    if (firstRow === undefined) {
                        firstRow = row;
                    }
                    if (firstHThor === undefined && (row as any).type === "hthor") {
                        firstHThor = row;
                    }
                    if (firstThor === undefined && (row as any).type === "thor") {
                        firstThor = row;
                    }
                    return row;
                });
                if (autoSelectDropdown(props.selectedKey, props.required, props.optional)) {
                    const selRow = firstThor || firstHThor || firstRow;
                    setDefaultRow(selRow);
                }
            }
        }).catch(e => logger.error);
        return () => { active = false; };
    }, [props.selectedKey, props.required, props.optional]);

    return <AsyncDropdown {...props} selectedKey={props.selectedKey || defaultRow?.key as string} options={targetClusters} />;
};

export interface TargetDropzoneTextFieldProps extends AsyncDropdownProps {
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
        }).catch(e => logger.error);
    }, []);

    return <AsyncDropdown {...props} options={targetDropzones} />;
};

export interface TargetServerTextFieldProps extends AsyncDropdownProps {
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
        }).catch(e => logger.error);
    }, [props.selectedKey, props.dropzone]);

    return <AsyncDropdown {...props} options={targetServers} />;
};

export interface TargetServerTextFieldLinkedProps extends AsyncDropdownProps {
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

export interface TargetGroupTextFieldProps extends AsyncDropdownProps {
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
        }).catch(e => logger.error);
    }, []);

    return <AsyncDropdown {...props} options={targetGroups} />;
};

export interface TargetDfuSprayQueueTextFieldProps extends AsyncDropdownProps {
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
        }).catch(e => logger.error);
    }, []);

    return <AsyncDropdown {...props} options={dfuSprayQueues} />;
};

export interface TargetFolderTextFieldProps extends AsyncDropdownProps {
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
        if (props.optional) {
            retVal.push({ key: "", text: "" });
        }
        let _path = [Path, ""].join(pathSepChar).replace(machineDirectory, "");
        _path = (_path.length > 1 && _path.substr(-1) === "/") ? _path.substr(0, _path.length - 1) : _path;
        retVal.push({ key: Path, text: _path });
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
                    }).catch(e => logger.error);
                });
            }
        });
    }, [machineDirectory, props.optional]);

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

export interface UserGroupsProps extends AsyncDropdownProps {
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
            }).catch(logger.error);
    }, [props.username]);

    return <AsyncDropdown {...props} options={groups} />;
};

export interface GroupMembersProps extends AsyncDropdownProps {
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
        }).catch(logger.error);
    }, [props.groupname]);

    return <AsyncDropdown {...props} options={users} />;
};

export interface PermissionTypeProps extends AsyncDropdownProps {
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
        }).catch(logger.error);
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
                    field: <MaterialUITextField
                        key={fieldID}
                        type="datetime-local"
                        name={fieldID}
                        value={field.value}
                        placeholder={field.placeholder}
                        onChange={ev => {
                            field.value = ev.target.value;
                        }}
                        InputLabelProps={{ shrink: true }
                        }
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
                        optional
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
