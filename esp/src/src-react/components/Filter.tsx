import * as React from "react";
import { getTheme, mergeStyleSets, FontWeights, IDragOptions, IIconProps, ContextualMenu, DefaultButton, PrimaryButton, IconButton, Checkbox, Dropdown, IStackStyles, Modal, Stack, TextField, IDropdownProps, IDropdownOption } from "@fluentui/react";
import { useId } from "@fluentui/react-hooks";
import { TextField as MaterialUITextField } from "@material-ui/core";
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
    LogicalFileType | DFUWorkunitStateField;

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

export const TargetGroupTextField: React.FunctionComponent<IDropdownProps> = (props) => {

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
                field.value = ev.target.checked;
                setLocalFields({ ...localFields });
                break;
            default:
                field.value = ev.target.value;
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
                formFields.push(
                    <TextField 
                        key={fieldID} 
                        label={field.label} 
                        type="string" 
                        name={fieldID} 
                        value={field.value} 
                        placeholder={field.placeholder} 
                        onChange={handleChange} 
                    />
                );
                break;
            case "checkbox":
                field.value = field.value || false;
                formFields.push(
                    <Checkbox 
                        key={fieldID} 
                        label={field.label} 
                        name={fieldID} 
                        checked={field.value === true ? true : false} 
                        onChange={handleChange} 
                    />
                );
                break;
            case "datetime":
                field.value = field.value || "";
                formFields.push(
                    <MaterialUITextField
                        key={fieldID}
                        label={field.label}
                        type="datetime-local"
                        name={fieldID}
                        value={field.value}
                        placeholder={field.placeholder}
                        onChange={handleChange}
                        InputLabelProps={{ shrink: true }}
                    />
                );
                break;
            case "workunit-state":
                field.value = field.value || "";
                formFields.push(
                    <Dropdown
                        key={fieldID}
                        label={field.label}
                        defaultSelectedKey={field.value}
                        options={states.map(state => {
                            return {
                                key: state,
                                text: state
                            };
                        })}
                        onChange={(ev, row) => {
                            localFields[fieldID].value = row.key as string;
                            setLocalFields({ ...localFields });
                        }}
                        placeholder={field.placeholder}
                    />
                );
                break;
            case "file-type":
                field.value = field.value || "";
                formFields.push(
                    <Dropdown
                        key={fieldID}
                        label={field.label}
                        defaultSelectedKey={field.value}
                        options={[
                            { key: "", text: nlsHPCC.LogicalFilesAndSuperfiles },
                            { key: "Logical Files Only", text: nlsHPCC.LogicalFilesOnly },
                            { key: "Superfiles Only", text: nlsHPCC.SuperfilesOnly },
                            { key: "Not in Superfiles", text: nlsHPCC.NotInSuperfiles },
                        ]}
                        onChange={(ev, row) => {
                            localFields[fieldID].value = row.key as string;
                            setLocalFields({ ...localFields });
                        }}
                        placeholder={field.placeholder}
                    />
                );
                break;
            case "file-sortby":
                field.value = field.value || "";
                formFields.push(
                    <Dropdown
                        key={fieldID}
                        label={field.label}
                        defaultSelectedKey={field.value}
                        options={[
                            { key: "", text: "" },
                            { key: "Newest", text: nlsHPCC.Newest },
                            { key: "Oldest", text: nlsHPCC.Oldest },
                            { key: "Smallest", text: nlsHPCC.Smallest },
                            { key: "Largest", text: nlsHPCC.Largest }
                        ]}
                        onChange={(ev, row) => {
                            localFields[fieldID].value = row.key as string;
                            setLocalFields({ ...localFields });
                        }}
                        placeholder={field.placeholder}
                    />
                );
                break;
            case "queries-suspend-state":
                field.value = field.value || "";
                formFields.push(
                    <Dropdown
                        key={fieldID}
                        label={field.label}
                        defaultSelectedKey={field.value}
                        options={[
                            { key: "", text: "" },
                            { key: "Not suspended", text: nlsHPCC.NotSuspended },
                            { key: "Suspended", text: nlsHPCC.Suspended },
                            { key: "Suspended by user", text: nlsHPCC.SuspendedByUser },
                            { key: "Suspended by first node", text: nlsHPCC.SuspendedByFirstNode },
                            { key: "Suspended by any node", text: nlsHPCC.SuspendedByAnyNode },
                        ]}
                        onChange={(ev, row) => {
                            localFields[fieldID].value = row.key as string;
                            setLocalFields({ ...localFields });
                        }}
                        placeholder={field.placeholder}
                    />
                );
                break;
            case "queries-active-state":
                field.value = field.value || "";
                formFields.push(
                    <Dropdown
                        key={fieldID}
                        label={field.label}
                        defaultSelectedKey={field.value}
                        options={[
                            { key: "", text: "" },
                            { key: "1", text: nlsHPCC.Active },
                            { key: "0", text: nlsHPCC.NotActive }
                        ]}
                        onChange={(ev, row) => {
                            localFields[fieldID].value = row.key as string;
                            setLocalFields({ ...localFields });
                        }}
                        placeholder={field.placeholder}
                    />
                );
                break;
            case "target-cluster":
                field.value = field.value || "";
                formFields.push(
                    <TargetClusterTextField
                        key={fieldID}
                        label={field.label}
                        defaultSelectedKey={field.value}
                        onChange={(ev, row) => {
                            localFields[fieldID].value = row.key as string;
                            setLocalFields({ ...localFields });
                        }}
                        placeholder={field.placeholder}
                        options={[]}
                    />
                );
                break;
            case "target-group":
                field.value = field.value || "";
                formFields.push(
                    <TargetGroupTextField
                        key={fieldID}
                        label={field.label}
                        defaultSelectedKey=""
                        onChange={(ev, row) => {
                            localFields[fieldID].value = row.key as string;
                            setLocalFields({ ...localFields });
                        }}
                        placeholder={field.placeholder}
                        options={[]}
                    />
                );
                break;
            case "dfuworkunit-state":
                field.value = field.value || "";
                formFields.push(
                    <Dropdown
                        key={fieldID}
                        label={field.label}
                        defaultSelectedKey=""
                        options={dfustates.map(state => {
                            return {
                                key: state,
                                text: state
                            };
                        })}
                        onChange={(ev, row) => {
                            localFields[fieldID].value = row.key as string;
                            setLocalFields({ ...localFields });
                        }}
                        placeholder={field.placeholder}
                    />
                );
                break;
            case "logicalfile-type":
                field.value = field.value || "Created";
                formFields.push(
                    <Dropdown
                        key={fieldID}
                        label={field.label}
                        defaultSelectedKey=""
                        options={[
                            { key: "Created", text: nlsHPCC.CreatedByWorkunit },
                            { key: "Used", text: nlsHPCC.UsedByWorkunit }
                        ]}
                        onChange={(ev, row) => {
                            localFields[fieldID].value = row.key as string;
                            setLocalFields({ ...localFields });
                        }}
                        placeholder={field.placeholder}
                    />
                );
                break;
        }
    }

    return <>
        {...formFields}
    </>;
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

    const titleId = useId("title");

    const dragOptions: IDragOptions = {
        moveMenuItemText: "Move",
        closeMenuItemText: "Close",
        menu: ContextualMenu,
    };

    const theme = getTheme();

    const contentStyles = mergeStyleSets({
        container: {
            display: "flex",
            flexFlow: "column nowrap",
            alignItems: "stretch",
        },
        header: [
            {
                flex: "1 1 auto",
                borderTop: `4px solid ${theme.palette.themePrimary}`,
                color: theme.palette.neutralPrimary,
                display: "flex",
                alignItems: "center",
                fontWeight: FontWeights.semibold,
                padding: "12px 12px 14px 24px",
            },
        ],
        body: {
            flex: "4 4 auto",
            padding: "0 24px 24px 24px",
            overflowY: "hidden",
            selectors: {
                p: { margin: "14px 0" },
                "p:first-child": { marginTop: 0 },
                "p:last-child": { marginBottom: 0 },
            },
        },
    });

    const cancelIcon: IIconProps = { iconName: "Cancel" };
    const iconButtonStyles = {
        root: {
            color: theme.palette.neutralPrimary,
            marginLeft: "auto",
            marginTop: "4px",
            marginRight: "2px",
        },
        rootHovered: {
            color: theme.palette.neutralDark,
        },
    };
    const buttonStackStyles: IStackStyles = {
        root: {
            height: "56px",
        },
    };
    return <Modal
        titleAriaId={titleId}
        isOpen={showFilter}
        onDismiss={closeFilter}
        isBlocking={false}
        containerClassName={contentStyles.container}
        dragOptions={dragOptions}
    >
        <div className={contentStyles.header}>
            <span id={titleId}>Filter</span>
            <IconButton
                styles={iconButtonStyles}
                iconProps={cancelIcon}
                ariaLabel="Close popup modal"
                onClick={closeFilter}
            />
        </div>
        <div className={contentStyles.body}>
            <Stack>
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
            </Stack>
            <Stack
                horizontal
                horizontalAlign="space-between"
                verticalAlign="end"
                styles={buttonStackStyles}
            >
                <DefaultButton
                    text={nlsHPCC.Clear}
                    onClick={() => {
                        setDoReset(true);
                    }}
                />
                <PrimaryButton
                    text={nlsHPCC.Apply}
                    onClick={() => {
                        setDoSubmit(true);
                        closeFilter();
                    }}
                />
            </Stack>
        </div>
    </Modal>;
};
