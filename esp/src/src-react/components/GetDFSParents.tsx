import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { daliService } from "../comms/dali";
import { useDaliResult } from "../hooks/useDaliResult";
import { DaliAdminForm } from "./DaliAdminForm";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/GetDFSParents.tsx");

export const GetDFSParents: React.FunctionComponent = () => {
    const { columns, items, setResult } = useDaliResult();
    const [fileName, setFileName] = React.useState("");

    const onSubmit = React.useCallback(() => {
        daliService.GetDFSParents({ FileName: fileName })
            .then(response => setResult(response.Result))
            .catch(err => logger.error(err));
    }, [fileName, setResult]);

    return <DaliAdminForm
        fields={{ "LogicalNameMask": { label: nlsHPCC.FileName, type: "string", value: fileName } }}
        onChange={(_, v) => setFileName(v)}
        onSubmit={onSubmit}
        columns={columns}
        items={items}
    />;
};