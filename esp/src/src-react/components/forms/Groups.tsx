import * as React from "react";
import { Label, makeStyles } from "@fluentui/react-components";
import { createInputs, Fields } from "./Fields";

interface FieldsTableProps {
    fields: Fields;
    width?: string;
    onChange?: (id: string, newValue: any) => void;
}

const useTableGroupStyles = makeStyles({
    root: {
        padding: "4px",
        minWidth: "66%",
        "& .ms-TextField-field[readonly]": { padding: 0 },
        "& .ms-Link": { paddingLeft: "0 !important" }
    },
    labelCell: { padding: "5px 0", whiteSpace: "nowrap", fontWeight: 600 },
    fieldCell: { width: "80%", paddingLeft: "8px" }
});

const useMultiColumnStyles = makeStyles({
    root: {
        borderCollapse: "collapse",
        minWidth: "80%",
        "& tbody tr:nth-child(odd)": { backgroundColor: "var(--colorNeutralBackground3)" },
        "& th": {
            padding: "0 1em 1em 1em",
            fontSize: "1em",
            fontWeight: 600
        },
        "& td": {
            padding: "0.85em 1em",
            fontSize: "1em",
            lineHeight: "1em"
        }
    },
    wrapper: { padding: "0 0 1em 0.3em" },
    label: { marginBottom: 0 }
});

export const TableGroup: React.FunctionComponent<FieldsTableProps> = ({
    fields,
    width = "initial",
    onChange = (id: string, newValue: any) => { }
}) => {

    const formFields: { id: string, label: string, field: any }[] = createInputs(fields, onChange);

    const tableGroupClasses = useTableGroupStyles();

    return <table className={tableGroupClasses.root} style={{ width }}>
        <tbody>
            {formFields.map((ff) => {
                return <tr key={ff.id}>
                    <td className={tableGroupClasses.labelCell}><Label htmlFor={ff.id}>{ff.label}</Label></td>
                    <td className={tableGroupClasses.fieldCell}>{ff.field}</td>
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
    const multiColumnClasses = useMultiColumnStyles();

    return <div className={multiColumnClasses.wrapper}>
        {label && <h3 className={multiColumnClasses.label}>{label}</h3>}
        <table className={multiColumnClasses.root}>
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