import * as React from "react";
import { FormGroup, TextField, FormControlLabel, Checkbox, Dialog, DialogTitle, DialogContent, DialogActions, Button, MenuItem, TextFieldProps } from "@material-ui/core";
import { Topology, TpLogicalClusterQuery } from "@hpcc-js/comms";
import { TpGroupQuery } from "src/WsTopology";
import { States } from "src/WsWorkunits";
import { States as DFUStates } from "src/FileSpray";
import nlsHPCC from "src/nlsHPCC";

type FieldType = "string" | "checkbox" | "datetime" |
    "workunit-state" |
    "file-type" | "file-sortby" |
    "queries-suspend-state" | "queries-active-state" |
    "target-cluster" | "target-group" |
    "logicalfile-type" |
    "dfuworkunit-state"
    ;

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

type Field = StringField | CheckboxField | DateTimeField |
    WorkunitStateField |
    FileTypeField | FileSortByField |
    QueriesSuspendStateField | QueriesActiveStateField |
    TargetClusterField | TargetGroupField |
    LogicalFileType |
    DFUWorkunitStateField;
export type Fields = { [name: string]: Field };
export type Values = { [name: string]: string | number | boolean | (string | number | boolean)[] };

const fieldsToRequest = (fields: Fields) => {
    const retVal: Values = {};
    for (const name in fields) {
        if (!fields[name].disabled(fields)) {
            retVal[name] = fields[name].value;
        }
    }
    return retVal;
};

const TargetClusterTextField: React.FunctionComponent<TextFieldProps> = (props) => {

    const [targetClusters, setTargetClusters] = React.useState<TpLogicalClusterQuery.TpLogicalCluster[]>([]);

    React.useEffect(() => {
        const topology = new Topology({ baseUrl: "" });
        topology.fetchLogicalClusters().then(response => {
            setTargetClusters([{ Name: "", Type: "", LanguageVersion: "", Process: "", Queue: "" }, ...response]);
        });
    }, []);

    return <TextField {...props} >
        {targetClusters.map(tc => <MenuItem key={tc.Name} value={tc.Name}>{tc.Name}{tc.Name !== tc.Type ? ` (${tc.Type})` : ""}</MenuItem>)}
    </TextField>;
};

export const TargetGroupTextField: React.FunctionComponent<TextFieldProps> = (props) => {

    const [targetGroups, setTargetGroups] = React.useState([]);

    React.useEffect(() => {
        TpGroupQuery({}).then(({ TpGroupQueryResponse }) => {
            setTargetGroups(TpGroupQueryResponse.TpGroups.TpGroup);
        });
    }, []);

    return <TextField {...props} >
        {targetGroups.map(tc => <MenuItem key={tc.Name} value={tc.Name}>{tc.Name}{tc.Name !== tc.Kind ? ` (${tc.Kind})` : ""}</MenuItem>)}
    </TextField>;
};

interface FormContentProps {
    fields: Fields;
    doSubmit: boolean;
    doReset: boolean;
    onSubmit: (fields: Values) => void;
    onReset: (fields: Values) => void;
}

export const FormContent: React.FunctionComponent<FormContentProps> = ({
    fields,
    doSubmit,
    doReset,
    onSubmit,
    onReset
}) => {

    const [localFields, setLocalFields] = React.useState<Fields>({ ...fields });

    React.useEffect(() => {
        if (doSubmit === false) return;
        onSubmit(fieldsToRequest(localFields));
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [doSubmit]);

    React.useEffect(() => {
        if (doReset === false) return;
        for (const key in localFields) {
            delete localFields[key].value;
        }
        setLocalFields(localFields);
        onReset(fieldsToRequest(localFields));
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [doReset]);

    const handleChange = ev => {
        const field = localFields[ev.target.name];
        switch (field.type) {
            case "checkbox":
                localFields[ev.target.name].value = ev.target.checked;
                setLocalFields({ ...localFields });
                break;
            default:
                localFields[ev.target.name].value = ev.target.value;
                setLocalFields({ ...localFields });
                break;
        }
    };

    const formFields = [];
    for (const fieldID in localFields) {
        const field: Field = localFields[fieldID];
        if (!field.disabled) {
            field.disabled = () => false;
        }
        switch (field.type) {
            case "string":
                field.value = field.value || "";
                formFields.push(<TextField key={fieldID} label={field.label} type="string" name={fieldID} value={field.value} placeholder={field.placeholder} onChange={handleChange} />);
                break;
            case "checkbox":
                field.value = field.value || false;
                formFields.push(<FormControlLabel key={fieldID} label={field.label} name={fieldID} control={
                    <Checkbox checked={field.value === true ? true : false} onChange={handleChange} />
                } />);
                break;
            case "datetime":
                field.value = field.value || "";
                formFields.push(<TextField key={fieldID} label={field.label} type="datetime-local" name={fieldID} value={field.value} placeholder={field.placeholder} onChange={handleChange} InputLabelProps={{ shrink: true }} />);
                break;
            case "workunit-state":
                field.value = field.value || "";
                formFields.push(
                    <TextField key={fieldID} label={field.label} select name={fieldID} value={field.value} placeholder={field.placeholder} onChange={handleChange} >
                        {states.map(state => <MenuItem key={state} value={state}>{state}</MenuItem>)}
                    </TextField>
                );
                break;
            case "file-type":
                field.value = field.value || "";
                formFields.push(
                    <TextField key={fieldID} label={field.label} select name={fieldID} value={field.value} placeholder={field.placeholder} onChange={handleChange} >
                        <MenuItem key={""} value="">{nlsHPCC.LogicalFilesAndSuperfiles}</MenuItem>
                        <MenuItem key={"Logical Files Only"} value="Logical Files Only">{nlsHPCC.LogicalFilesOnly}</MenuItem>
                        <MenuItem key={"Superfiles Only"} value="Superfiles Only">{nlsHPCC.SuperfilesOnly}</MenuItem>
                        <MenuItem key={"Not in Superfiles"} value="Not in Superfiles">{nlsHPCC.NotInSuperfiles}</MenuItem>
                    </TextField>
                );
                break;
            case "file-sortby":
                field.value = field.value || "";
                formFields.push(
                    <TextField key={fieldID} label={field.label} select name={fieldID} value={field.value} placeholder={field.placeholder} onChange={handleChange} >
                        <MenuItem key={""} value="">&nbsp;</MenuItem>
                        <MenuItem key={"Newest"} value="Newest">{nlsHPCC.Newest}</MenuItem>
                        <MenuItem key={"Oldest"} value="Oldest">{nlsHPCC.Oldest}</MenuItem>
                        <MenuItem key={"Smallest"} value="Smallest">{nlsHPCC.Smallest}</MenuItem>
                        <MenuItem key={"Largest"} value="Largest">{nlsHPCC.Largest}</MenuItem>
                    </TextField>
                );
                break;
            case "queries-suspend-state":
                field.value = field.value || "";
                formFields.push(
                    <TextField key={fieldID} label={field.label} select name={fieldID} value={field.value} placeholder={field.placeholder} onChange={handleChange} >
                        <MenuItem key={""} value="">&nbsp;</MenuItem>
                        <MenuItem key={"Not suspended"} value="Not suspended">{nlsHPCC.NotSuspended}</MenuItem>
                        <MenuItem key={"Suspended"} value="Suspended">{nlsHPCC.Suspended}</MenuItem>
                        <MenuItem key={"Suspended by user"} value="Suspended by user">{nlsHPCC.SuspendedByUser}</MenuItem>
                        <MenuItem key={"Suspended by first node"} value="Suspended by first node">{nlsHPCC.SuspendedByFirstNode}</MenuItem>
                        <MenuItem key={"Suspended by any node"} value="Suspended by any node">{nlsHPCC.SuspendedByAnyNode}</MenuItem>
                    </TextField>
                );
                break;
            case "queries-active-state":
                field.value = field.value || "";
                formFields.push(
                    <TextField key={fieldID} label={field.label} select name={fieldID} value={field.value} placeholder={field.placeholder} onChange={handleChange} >
                        <MenuItem key={""} value="">&nbsp;</MenuItem>
                        <MenuItem key={"1"} value="1">{nlsHPCC.Active}</MenuItem>
                        <MenuItem key={"0"} value="0">{nlsHPCC.NotActive}</MenuItem>
                    </TextField>
                );
                break;
            case "target-cluster":
                field.value = field.value || "";
                formFields.push(<TargetClusterTextField key={fieldID} label={field.label} select name={fieldID} value={field.value} placeholder={field.placeholder} onChange={handleChange} />);
                break;
            case "target-group":
                field.value = field.value || "";
                formFields.push(<TargetGroupTextField key={fieldID} label={field.label} select name={fieldID} value={field.value} placeholder={field.placeholder} onChange={handleChange} />);
                break;
            case "logicalfile-type":
                field.value = field.value || "Created";
                formFields.push(
                    <TextField key={fieldID} label={field.label} select name={fieldID} value={field.value} disabled={field.disabled(localFields)} placeholder={field.placeholder} onChange={handleChange} >
                        <MenuItem key={"Created"} value="Created">{nlsHPCC.CreatedByWorkunit}</MenuItem>
                        <MenuItem key={"Used"} value="Used">{nlsHPCC.UsedByWorkunit}</MenuItem>
                    </TextField>
                );
                break;
            case "dfuworkunit-state":
                field.value = field.value || "";
                formFields.push(
                    <TextField key={fieldID} label={field.label} select name={fieldID} value={field.value} placeholder={field.placeholder} onChange={handleChange} >
                        {dfustates.map(state => <MenuItem key={state} value={state}>{state}</MenuItem>)}
                    </TextField>
                );
                break;

        }
    }

    return <FormGroup style={{ minWidth: "320px" }}>
        {...formFields}
    </FormGroup >;
};

interface FilterProps {
    filterFields: Fields;
    onApply: (values: Values) => void;

    showFilter: boolean;
    setShowFilter: (_: boolean) => void;
}

export const Filter: React.FunctionComponent<FilterProps> = ({
    filterFields,
    onApply,
    showFilter,
    setShowFilter
}) => {

    const [doSubmit, setDoSubmit] = React.useState(false);
    const [doReset, setDoReset] = React.useState(false);

    const closeFilter = () => setShowFilter(false);

    return <Dialog onClose={closeFilter} aria-labelledby="simple-dialog-title" open={showFilter} >
        <DialogTitle id="form-dialog-title">{nlsHPCC.Filter}</DialogTitle>
        <DialogContent>
            <FormContent
                fields={filterFields}
                doSubmit={doSubmit}
                doReset={doReset}
                onSubmit={fields => {
                    setDoSubmit(false);
                    onApply(fields);
                }}
                onReset={() => {
                    setDoReset(false);
                }}
            />
        </DialogContent>
        <DialogActions>
            <Button variant="contained" color="primary" onClick={() => {
                setDoSubmit(true);
                closeFilter();
            }} >
                {nlsHPCC.Apply}
            </Button>
            <Button variant="contained" color="secondary" onClick={() => {
                setDoReset(true);
            }} >
                {nlsHPCC.Clear}
            </Button>
        </DialogActions>
    </Dialog>;
};
