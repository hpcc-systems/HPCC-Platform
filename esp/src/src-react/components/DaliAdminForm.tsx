import * as React from "react";
import { Button, Table, TableHeader, TableRow, TableHeaderCell, TableBody, TableCell, makeStyles } from "@fluentui/react-components";
import { HolyGrail } from "../layouts/HolyGrail";
import { SizeMe } from "../layouts/SizeMe";
import { TableGroup } from "./forms/Groups";
import { Fields } from "./forms/Fields";
import { DaliColumn } from "../hooks/useDaliResult";
import nlsHPCC from "src/nlsHPCC";

const useStyles = makeStyles({
    tableContainer: {
        position: "relative",
        width: "100%",
        height: "100%",
    },
    tableScroll: {
        position: "absolute",
        width: "100%",
        overflow: "auto",
    },
    table: {
        width: "100%",
        tableLayout: "fixed",
    },
});

interface DaliAdminFormProps {
    fields: Fields;
    onChange: (id: string, value: any) => void;
    onSubmit: () => void;
    columns: DaliColumn[];
    items: object[];
    confirmDialog?: React.ReactNode;
}

export const DaliAdminForm: React.FunctionComponent<DaliAdminFormProps> = ({
    fields,
    onChange,
    onSubmit,
    columns,
    items,
    confirmDialog,
}) => {
    const styles = useStyles();

    const onKeyDown = React.useCallback((evt: React.KeyboardEvent) => {
        if (evt.key === "Enter") {
            onSubmit();
        }
    }, [onSubmit]);

    return <HolyGrail
        header={<span onKeyDown={onKeyDown}>
            <TableGroup fields={fields} onChange={onChange} />
            <Button onClick={onSubmit} appearance="primary">{nlsHPCC.Submit}</Button>
        </span>}
        main={<SizeMe>{({ size }) => {
            return <div className={styles.tableContainer}>
                <div className={styles.tableScroll} style={{ height: `${size.height}px` }}>
                    <Table className={styles.table} size="small">
                        <TableHeader>
                            <TableRow>
                                {columns.map(col => (
                                    <TableHeaderCell key={col.key} style={{ minWidth: col.minWidth }}>
                                        {col.name}
                                    </TableHeaderCell>
                                ))}
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {items.map((item, idx) => (
                                <TableRow key={idx}>
                                    {columns.map(col => (
                                        <TableCell key={col.key}>
                                            {item[col.fieldName] ?? ""}
                                        </TableCell>
                                    ))}
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                    {confirmDialog}
                </div>
            </div>;
        }}</SizeMe>}
    />;
};
