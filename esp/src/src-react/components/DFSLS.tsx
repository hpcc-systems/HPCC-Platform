import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { daliService } from "../comms/dali";
import { useDaliResult } from "../hooks/useDaliResult";
import { useFormFields } from "../hooks/useFormFields";
import { DaliAdminForm } from "./DaliAdminForm";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/DFSLS.tsx");

export const DFSLS: React.FunctionComponent = () => {
    const { columns, items, setResult } = useDaliResult();
    const [fields, handleChange] = useFormFields({
        Name: "",
        PathAndNameOnly: true,
        IncludeSubFileInfo: false,
        Recursively: false,
    });

    const onSubmit = React.useCallback(() => {
        daliService.DFSLS({
            Name: fields.Name,
            PathAndNameOnly: fields.PathAndNameOnly,
            IncludeSubFileInfo: fields.IncludeSubFileInfo,
            Recursively: fields.Recursively,
        })
            .then(response => setResult(response.Result))
            .catch(err => logger.error(err));
    }, [fields, setResult]);

    return <DaliAdminForm
        fields={{
            "Name": { label: nlsHPCC.Name, type: "string", value: fields.Name },
            "PathAndNameOnly": { label: nlsHPCC.PathAndNameOnly, type: "checkbox", value: fields.PathAndNameOnly },
            "IncludeSubFileInfo": { label: nlsHPCC.IncludeSubFileInfo, type: "checkbox", value: fields.IncludeSubFileInfo },
            "Recursively": { label: nlsHPCC.Recursively, type: "checkbox", value: fields.Recursively },
        }}
        onChange={handleChange}
        onSubmit={onSubmit}
        columns={columns}
        items={items}
    />;
};