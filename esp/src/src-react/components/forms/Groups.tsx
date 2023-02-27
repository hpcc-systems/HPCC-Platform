import * as React from "react";
import { Label, mergeStyleSets } from "@fluentui/react";
import { createInputs, Fields } from "./Fields";
import { useUserTheme } from "../../hooks/theme";

interface FieldsTableProps {
    fields: Fields;
    onChange?: (id: string, newValue: any) => void;
}

const tableGroupStyles = mergeStyleSets({
    root: {
        padding: 4,
        minWidth: "66%",
        selectors: {
            ".ms-Textfield": {
                width: "80%"
            },
            ".ms-TextField-field[readonly]": {
                padding: 0
            },
            ".ms-Link": {
                paddingLeft: "0 !important"
            }
        }
    }
});

export const TableGroup: React.FunctionComponent<FieldsTableProps> = ({
    fields,
    onChange = (id: string, newValue: any) => { }
}) => {

    const formFields: { id: string, label: string, field: any }[] = createInputs(fields, onChange);

    return <table className={tableGroupStyles.root}>
        <tbody>
            {formFields.map((ff) => {
                return <tr key={ff.id}>
                    <td style={{ whiteSpace: "nowrap" }}><Label htmlFor={ff.id}>{ff.label}</Label></td>
                    <td style={{ width: "80%", paddingLeft: 8 }}>{ff.field}</td>
                </tr>;
            })}
        </tbody>
    </table>;
};

interface MultiColumnTableGroupProps {
    label?: string;
    columns: string[];
    rows: object[];
}

export const MultiColumnTableGroup: React.FunctionComponent<MultiColumnTableGroupProps> = ({
    label = "",
    columns,
    rows
}) => {
    const { theme } = useUserTheme();

    const tableClasses = React.useMemo(() => mergeStyleSets({
        root: {
            borderCollapse: "collapse",
            minWidth: "80%",
            selectors: {
                "tbody tr:nth-child(odd)": {
                    backgroundColor: theme.palette.neutralLighter
                },
                "th": {
                    padding: label ? "0 1em 1em 1em" : "1em",
                    fontSize: "1em",
                    fontWeight: 600
                },
                "td": {
                    padding: "0.85em 1em",
                    fontSize: "1em",
                    lineHeight: "1em"
                }
            }
        }
    }), [label, theme]);

    return <div style={{ padding: "0 0 1em 0.3em" }}>
        {label && <h3 style={{ marginBottom: 0 }}>{label}</h3>}
        <table className={tableClasses.root}>
            <thead>
                <tr>
                    {columns.length < Object.keys(rows[0]).length && <th></th>}
                    {columns.map(column => {
                        return <th key={column}>{column}</th>;
                    })}
                </tr>
            </thead>
            <tbody>
                {rows.map((row, idx) => {
                    return <tr key={`row_${idx}`}>
                        {Object.keys(row).map((key, idx) => {
                            return <td key={`${key}_${idx}`}>{row[key]}</td>;
                        })}
                    </tr>;
                })}
            </tbody>
        </table>
    </div>;
};

export const SimpleGroup: React.FunctionComponent<FieldsTableProps> = ({
    fields,
    onChange = (id: string, newValue: any) => { }
}) => {

    const formFields: { id: string, label: string, field: any }[] = createInputs(fields, onChange);

    return <>
        {
            formFields.map((ff) => {
                return <>
                    <Label htmlFor={ff.id}>{ff.label}</Label>
                    {ff.field}
                </>;
            })
        }
    </>;
};