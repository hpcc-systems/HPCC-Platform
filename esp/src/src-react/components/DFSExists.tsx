import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { daliService } from "../comms/dali";
import { useDaliResult } from "../hooks/useDaliResult";
import { DaliAdminForm } from "./DaliAdminForm";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/DFSExists.tsx");

export const DFSExists: React.FunctionComponent = () => {
    const { columns, items, setColumns, setItems } = useDaliResult();
    const [fileName, setFileName] = React.useState("");

    const onSubmit = React.useCallback(() => {
        daliService.DFSExists({ FileName: fileName })
            .then(response => {
                setColumns([{ key: "Result", name: "Result", fieldName: "result", minWidth: 100 }]);
                setItems([{ key: "Result", result: String(response.Result) }]);
            })
            .catch(err => logger.error(err));
    }, [fileName, setColumns, setItems]);

    return <DaliAdminForm
        fields={{ "FileName": { label: nlsHPCC.FileName, type: "string", value: fileName } }}
        onChange={(_, v) => setFileName(v)}
        onSubmit={onSubmit}
        columns={columns}
        items={items}
    />;
};
