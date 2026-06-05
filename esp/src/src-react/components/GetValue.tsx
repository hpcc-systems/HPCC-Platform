import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { daliService } from "../comms/dali";
import { useDaliResult } from "../hooks/useDaliResult";
import { DaliAdminForm } from "./DaliAdminForm";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/GetValue.tsx");

export const GetValue: React.FunctionComponent = () => {
    const { columns, items, setResult } = useDaliResult();
    const [path, setPath] = React.useState("");

    const onSubmit = React.useCallback(() => {
        daliService.GetValue({ Path: path })
            .then(response => setResult(response.Result))
            .catch(err => logger.error(err));
    }, [path, setResult]);

    return <DaliAdminForm
        fields={{ "Path": { label: nlsHPCC.Path, type: "string", value: path } }}
        onChange={(_, v) => setPath(v)}
        onSubmit={onSubmit}
        columns={columns}
        items={items}
    />;
};
