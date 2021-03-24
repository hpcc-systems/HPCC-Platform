import * as React from "react";
import { Checkbox, Dropdown, TextField, IDropdownProps, IDropdownOption, Label, Link } from "@fluentui/react";
import { TextField as MaterialUITextField } from "@material-ui/core";
import { Topology, TpLogicalClusterQuery } from "@hpcc-js/comms";
import { TpGroupQuery } from "src/WsTopology";
import { States } from "src/WsWorkunits";
import { States as DFUStates } from "src/FileSpray";
import nlsHPCC from "src/nlsHPCC";

type FieldType = "string" | "checkbox" | "datetime" | "link" |
    "workunit-state" |
    "file-type" | "file-sortby" |
    "queries-suspend-state" | "queries-active-state" |
    "target-cluster" | "target-group" |
    "logicalfile-type" | "dfuworkunit-state";

const states = Object.keys(States).map(s => States[s]);
const dfustates = Object.keys(DFUStates).map(s => DFUStates[s]);

interface BaseField {
    type: FieldType;
    label: string;
    disabled?: (params) => boolean;
    placeholder?: string;
}

interface StringField extends BaseField {
    type: "string";
    value?: string;
    readonly?: boolean;
    multiline?: boolean;
}

interface DateTimeField extends BaseField {
    type: "datetime";
    value?: string;
}

interface LinkField extends BaseField {
    type: "link";
    href: string;
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

type Field = StringField | CheckboxField | DateTimeField | LinkField |
    WorkunitStateField |
    FileTypeField | FileSortByField |
    QueriesSuspendStateField | QueriesActiveStateField |
    TargetClusterField | TargetGroupField |
    LogicalFileType | DFUWorkunitStateField;

export type Fields = { [id: string]: Field };

const TargetClusterTextField: React.FunctionComponent<IDropdownProps> = (props) => {

    const [targetClusters, setTargetClusters] = React.useState<IDropdownOption[]>([]);

    React.useEffect(() => {
        const topology = new Topology({ baseUrl: "" });
        topology.fetchLogicalClusters().then((response: TpLogicalClusterQuery.TpLogicalCluster[]) => {
            setTargetClusters(
                [
                    { Name: "", Type: "", LanguageVersion: "", Process: "", Queue: "" },
                    ...response
                ]
                    .map(n => {
                        return {
                            key: n.Name,
                            text: n.Name + (n.Name !== n.Type ? ` (${n.Type})` : "")
                        };
                    })
            );
        });
    }, []);

    return <Dropdown
        {...props}
        options={targetClusters}
    />;
};

const TargetGroupTextField: React.FunctionComponent<IDropdownProps> = (props) => {

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

    return <Dropdown
        {...props}
        options={targetGroups}
    />;
};

interface DetailsProps {
    fields: Fields;
    onChange?: (id: string, newValue: any) => void;
}

export const Details: React.FunctionComponent<DetailsProps> = ({
    fields,
    onChange = (id: string, newValue: any) => { }
}) => {

    const formFields: { id: string, label: string, field: any }[] = [];
    for (const fieldID in fields) {
        const field = fields[fieldID];
        if (!field.disabled) {
            field.disabled = () => false;
        }
        switch (field.type) {
            case "string":
                field.value = field.value || "";
                formFields.push({
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
                formFields.push({
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
                formFields.push({
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
                formFields.push({
                    id: fieldID,
                    label: field.label,
                    field: <Link
                        key={fieldID}
                        href={field.href}
                        target="_blank"
                        style={{ paddingLeft: 8 }}>{field.href}</Link>
                });
                break;
            case "workunit-state":
                field.value = field.value || "";
                formFields.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        defaultSelectedKey={field.value}
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
                formFields.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        defaultSelectedKey={field.value}
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
                formFields.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        defaultSelectedKey={field.value}
                        options={[
                            { key: "", text: "" },
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
                formFields.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        defaultSelectedKey={field.value}
                        options={[
                            { key: "", text: "" },
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
                formFields.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        defaultSelectedKey={field.value}
                        options={[
                            { key: "", text: "" },
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
                formFields.push({
                    id: fieldID,
                    label: field.label,
                    field: <TargetClusterTextField
                        key={fieldID}
                        defaultSelectedKey={field.value}
                        onChange={(ev, row) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                        options={[]}
                    />
                });
                break;
            case "target-group":
                field.value = field.value || "";
                formFields.push({
                    id: fieldID,
                    label: field.label,
                    field: <TargetGroupTextField
                        key={fieldID}
                        defaultSelectedKey=""
                        onChange={(ev, row) => onChange(fieldID, row.key)}
                        placeholder={field.placeholder}
                        options={[]}
                    />
                });
                break;
            case "dfuworkunit-state":
                field.value = field.value || "";
                formFields.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        defaultSelectedKey=""
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
                formFields.push({
                    id: fieldID,
                    label: field.label,
                    field: <Dropdown
                        key={fieldID}
                        defaultSelectedKey=""
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

    return <table style={{ padding: 4 }}>
        <tbody>
            {formFields.map((ff) => {
                return <tr key={ff.id}>
                    <td style={{ whiteSpace: "nowrap" }}><Label>{ff.label}</Label></td>
                    <td style={{ width: "80%", paddingLeft: 8 }}>{ff.field}</td>
                </tr>;
            })}
        </tbody>
    </table>;
};
