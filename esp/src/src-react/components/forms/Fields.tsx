import * as React from "react";
import { Checkbox, Dropdown as DropdownBase, TextField, IDropdownOption, Link } from "@fluentui/react";
import { TextField as MaterialUITextField } from "@material-ui/core";
import { Topology, TpLogicalClusterQuery } from "@hpcc-js/comms";
import { TpDropZoneQuery, TpGroupQuery } from "src/WsTopology";
import { States } from "src/WsWorkunits";
import { FileList, States as DFUStates } from "src/FileSpray";
import nlsHPCC from "src/nlsHPCC";

interface DropdownProps {
    key?: string;
    label?: string;
    options?: IDropdownOption[];
    selectedKey?: string;
    optional?: boolean;
    required?: boolean;
    errorMessage?: string;
    onChange?: (event: React.FormEvent<HTMLDivElement>, option?: IDropdownOption, index?: number) => void;
    placeholder?: string;
    className?: string;
}

const Dropdown: React.FunctionComponent<DropdownProps> = ({
    key,
    label,
    options = [],
    selectedKey,
    optional = false,
    required = false,
    errorMessage,
    onChange,
    placeholder,
    className
}) => {

    const [selOptions, setSelOptions] = React.useState<IDropdownOption[]>([]);

    React.useEffect(() => {
        setSelOptions(optional ? [{ key: "", text: "" }, ...options] : [...options]);
    }, [optional, options, selectedKey]);

    return <DropdownBase key={key} label={label} errorMessage={errorMessage} required={required} className={className} defaultSelectedKey={selectedKey} onChange={onChange} placeholder={placeholder} options={selOptions} />;
};

export type FieldType = "string" | "number" | "checkbox" | "datetime" | "link" | "links" |
    "workunit-state" |
    "file-type" | "file-sortby" |
    "queries-suspend-state" | "queries-active-state" |
    "target-cluster" | "target-group" |
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
    type: "string";
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

interface LogicalFileType extends BaseField {
    type: "logicalfile-type";
    value?: string;
}

interface DFUWorkunitStateField extends BaseField {
    type: "dfuworkunit-state";
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

type Field = StringField | NumericField | CheckboxField | DateTimeField | LinkField | LinksField |
    WorkunitStateField |
    FileTypeField | FileSortByField |
    QueriesSuspendStateField | QueriesActiveStateField |
    TargetClusterField | TargetGroupField |
    LogicalFileType | DFUWorkunitStateField;

export type Fields = { [id: string]: Field };

export interface TargetClusterTextFieldProps extends DropdownProps {
    key: string;
    label?: string;
    selectedKey?: string;
    className?: string;
    onChange?: (event: React.FormEvent<HTMLDivElement>, option?: IDropdownOption, index?: number) => void;
    placeholder?: string;
}

export const TargetClusterTextField: React.FunctionComponent<TargetClusterTextFieldProps> = ({
    key,
    label,
    selectedKey,
    className,
    onChange,
    placeholder
}) => {

    const [targetClusters, setTargetClusters] = React.useState<IDropdownOption[]>([]);

    React.useEffect(() => {
        const topology = new Topology({ baseUrl: "" });
        topology.fetchLogicalClusters().then((response: TpLogicalClusterQuery.TpLogicalCluster[]) => {
            setTargetClusters(response
                .map((n, i) => {
                    return {
                        key: n.Name || "unknown",
                        text: n.Name + (n.Name !== n.Type ? ` (${n.Type})` : ""),
                    };
                })
            );
        });
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    return <Dropdown key={key} label={label} selectedKey={selectedKey} optional className={className} onChange={onChange} placeholder={placeholder} options={targetClusters} />;
};

export interface TargetDropzoneTextFieldProps extends DropdownProps {
    key: string;
    label?: string;
    selectedKey?: string;
    optional?: boolean;
    className?: string;
    onChange?: (event: React.FormEvent<HTMLDivElement>, option?: IDropdownOption, index?: number) => void;
    placeholder?: string;
}

export const TargetDropzoneTextField: React.FunctionComponent<TargetDropzoneTextFieldProps> = (props) => {

    const [targetDropzones, setTargetDropzones] = React.useState<IDropdownOption[]>([]);

    React.useEffect(() => {
        TpDropZoneQuery({}).then(({ TpDropZoneQueryResponse }) => {
            setTargetDropzones(
                TpDropZoneQueryResponse.TpDropZones.TpDropZone.map(n => {
                    return {
                        key: n.Name,
                        text: n.Name,
                        path: n.Path
                    };
                })
            );
        });
    }, []);

    return <Dropdown {...props} options={targetDropzones} />;
};
export interface TargetServerTextFieldProps extends DropdownProps {
    key: string;
    label?: string;
    className?: string;
    optional?: boolean;
    onChange?: (event: React.FormEvent<HTMLDivElement>, option?: IDropdownOption, index?: number) => void;
    placeholder?: string;
}

export const TargetServerTextField: React.FunctionComponent<TargetServerTextFieldProps> = (props) => {

    const [targetServers, setTargetServers] = React.useState<IDropdownOption[]>([]);

    React.useEffect(() => {
        // if (!selectedDropzone) return;
        TpDropZoneQuery({ Name: "" }).then(({ TpDropZoneQueryResponse }) => {
            setTargetServers(
                TpDropZoneQueryResponse.TpDropZones.TpDropZone[0].TpMachines.TpMachine.map(n => {
                    return {
                        key: n.Netaddress,
                        text: n.Netaddress,
                        OS: n.OS
                    };
                })
            );
        });
    }, []);

    return <Dropdown {...props} options={targetServers} />;
};

export interface TargetGroupTextFieldProps {
    key: string;
    label?: string;
    selectedKey?: string;
    className?: string;
    required?: boolean;
    optional?: boolean;
    errorMessage?: string;
    onChange?: (event: React.FormEvent<HTMLDivElement>, option?: IDropdownOption, index?: number) => void;
    placeholder?: string;
}

export const TargetGroupTextField: React.FunctionComponent<TargetGroupTextFieldProps> = (props) => {

    const [targetGroups, setTargetGroups] = React.useState<IDropdownOption[]>([]);

    React.useEffect(() => {
        TpGroupQuery({}).then(({ TpGroupQueryResponse }) => {
            setTargetGroups(
                TpGroupQueryResponse.TpGroups.TpGroup.map(n => {
                    return {
                        key: n.Name,
                        text: n.Name + (n.Name !== n.Kind ? ` (${n.Kind})` : "")
                    };
                })
            );
        });
    }, []);

    return <Dropdown {...props} options={targetGroups} />;
};

export interface TargetFolderTextFieldProps {
    key: string;
    label?: string;
    selectedKey?: string;
    pathSepChar?: string;
    machineAddress?: string;
    machineDirectory?: string;
    machineOS?: number;
    className?: string;
    required?: boolean;
    optional?: boolean;
    errorMessage?: string;
    onChange?: (event: React.FormEvent<HTMLDivElement>, option?: IDropdownOption, index?: number) => void;
    placeholder?: string;
}

export const TargetFolderTextField: React.FunctionComponent<TargetFolderTextFieldProps> = (props) => {

    const [folders, setFolders] = React.useState<IDropdownOption[]>([]);

    const { pathSepChar, machineAddress, machineDirectory, machineOS } = { ...props };

    const fetchFolders = React.useCallback((
        pathSepChar: string, Netaddr: string, Path: string, OS: number, depth: number
    ): Promise<IDropdownOption[]> => {
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
                    FileListResponse.files.PhysicalFileStruct.forEach(file => {
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
                    });
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

    return <Dropdown {...props} options={folders} />;
};

const states = Object.keys(States).map(s => States[s]);
const dfustates = Object.keys(DFUStates).map(s => DFUStates[s]);

export function createInputs(fields: Fields, onChange?: (id: string, newValue: any) => void) {
    const retVal: { id: string, label: string, field: any }[] = [];
    for (const fieldID in fields) {
        const field = fields[fieldID];
        if (!field.disabled) {
            field.disabled = () => false;
        }
        switch (field.type) {
            case "string":
                field.value = field.value || "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <TextField
                        key={fieldID}
                        type="string"
                        name={fieldID}
                        value={field.value}
                        placeholder={field.placeholder}
                        onChange={(evt, newValue) => onChange(fieldID, newValue)}
                        borderless={field.readonly && !field.multiline}
                        readOnly={field.readonly}
                        multiline={field.multiline}
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
                        checked={field.value === true ? true : false}
                        onChange={(evt, newValue) => onChange(fieldID, newValue)}
                    />
                });
                break;
            case "datetime":
                field.value = field.value || "";
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
            case "workunit-state":
                field.value = field.value || "";
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
                field.value = field.value || "";
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
                field.value = field.value || "";
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
            case "queries-suspend-state":
                field.value = field.value || "";
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
                field.value = field.value || "";
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
                field.value = field.value || "";
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
            case "target-group":
                field.value = field.value || "";
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
            case "dfuworkunit-state":
                field.value = field.value || "";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
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
                field.value = field.value || "Created";
                retVal.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        optional
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
