import * as React from "react";
import { getTheme, mergeStyleSets, FontWeights, IDragOptions, IIconProps, ContextualMenu, DefaultButton, PrimaryButton, IconButton, Dropdown, IStackStyles, Modal, Stack, IDropdownProps, IDropdownOption } from "@fluentui/react";
import { useId } from "@fluentui/react-hooks";
import { TpGroupQuery } from "src/WsTopology";
import nlsHPCC from "src/nlsHPCC";
import { Details } from "./Details";

type FieldType = "string" | "checkbox" | "datetime" |
    "workunit-state" |
    "file-type" | "file-sortby" |
    "queries-suspend-state" | "queries-active-state" |
    "target-cluster" | "target-group" |
    "logicalfile-type" | "dfuworkunit-state";

interface BaseField {
    type: FieldType;
    label: string;
    disabled?: (params) => boolean;
    placeholder?: string;
    readonly?: boolean;
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

    return <Details fields={localFields} onChange={(id, value) => {
        const field = localFields[id];
        switch (field.type) {
            case "checkbox":
                field.value = value;
                setLocalFields({ ...localFields });
                break;
            default:
                field.value = value;
                setLocalFields({ ...localFields });
                break;
        }
    }} />;
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
