import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { daliService } from "../comms/dali";
import { useDaliResult } from "../hooks/useDaliResult";
import { DaliAdminForm } from "./DaliAdminForm";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/DaliCount.tsx");

export const DaliCount: React.FunctionComponent = () => {
    const { columns, items, setColumns, setItems } = useDaliResult();
    const [path, setPath] = React.useState("");

    const onSubmit = React.useCallback(() => {
        daliService.Count({ Path: path })
            .then(response => {
                setItems([{ key: "Result", result: response.Result }]);
                setColumns([{ key: "Result", name: "Result", fieldName: "result", minWidth: 100 }]);
            })
            .catch(err => logger.error(err));
    }, [path, setColumns, setItems]);

    return <DaliAdminForm
        fields={{ "Path": { label: nlsHPCC.Path, type: "string", value: path } }}
        onChange={(_, v) => setPath(v)}
        onSubmit={onSubmit}
        columns={columns}
        items={items}
    />;
};
