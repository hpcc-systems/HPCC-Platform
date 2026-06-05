import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { daliService } from "../comms/dali";
import { useDaliResult } from "../hooks/useDaliResult";
import { DaliAdminForm } from "./DaliAdminForm";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/DFSCheck.tsx");

export const DFSCheck: React.FunctionComponent = () => {
    const { columns, items, setResult } = useDaliResult();
    const [dfsCheck, setDFSCheck] = React.useState(true);

    const onSubmit = React.useCallback(() => {
        daliService.DFSCheck({ DFSCheck: dfsCheck })
            .then(response => setResult(response.Result))
            .catch(err => logger.error(err));
    }, [dfsCheck, setResult]);

    return <DaliAdminForm
        fields={{ "DFSCheck": { label: nlsHPCC.DFSCheck, type: "checkbox", value: dfsCheck } }}
        onChange={(_, v) => setDFSCheck(v)}
        onSubmit={onSubmit}
        columns={columns}
        items={items}
    />;
};