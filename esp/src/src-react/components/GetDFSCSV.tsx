import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { daliService } from "../comms/dali";
import { useDaliResult } from "../hooks/useDaliResult";
import { DaliAdminForm } from "./DaliAdminForm";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/GetDFSCSV.tsx");

export const GetDFSCSV: React.FunctionComponent = () => {
    const { columns, items, setResult } = useDaliResult();
    const [logicalNameMask, setLogicalNameMask] = React.useState("");

    const onSubmit = React.useCallback(() => {
        daliService.GetDFSCSV({ LogicalNameMask: logicalNameMask })
            .then(response => setResult(response.Result))
            .catch(err => logger.error(err));
    }, [logicalNameMask, setResult]);

    return <DaliAdminForm
        fields={{ "LogicalNameMask": { label: nlsHPCC.LogicalNameMask, type: "string", value: logicalNameMask } }}
        onChange={(_, v) => setLogicalNameMask(v)}
        onSubmit={onSubmit}
        columns={columns}
        items={items}
    />;
};