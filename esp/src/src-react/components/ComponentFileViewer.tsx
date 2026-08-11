import * as React from "react";
import { TopologyService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { SourceEditor } from "./SourceEditor";

const logger = scopedLogger("src-react/components/ComponentFileViewer.tsx");
const service = new TopologyService({ baseUrl: "" });

export interface ComponentFileQueryParams {
    CompName?: string;
    CompType?: string;
    Directory?: string;
    FileType?: string;
    NetAddress?: string;
    OsType?: number;
}

interface ComponentFileViewerProps {
    queryParams?: ComponentFileQueryParams;
}

export const ComponentFileViewer: React.FunctionComponent<ComponentFileViewerProps> = ({
    queryParams
}) => {

    const [text, setText] = React.useState("");

    React.useEffect(() => {
        if (!queryParams) {
            setText("");
            return;
        }
        //  TpGetComponentFile returns the raw file body (not JSON), so request it as "text".
        //  Using the generated service method (which forces type "json") makes the comms layer
        //  fail to parse the XML and retry with a different credentials mode, yielding a spurious 401.
        service.connection().send("TpGetComponentFile", {
            CompName: queryParams?.CompName,
            CompType: queryParams?.CompType,
            Directory: queryParams?.Directory,
            FileType: queryParams?.FileType,
            NetAddress: queryParams?.NetAddress,
            OsType: queryParams?.OsType
        }, "text")
            .then((fileContents: string) => {
                setText(fileContents ?? "");
            })
            .catch(err => {
                logger.error(err);
            });
    }, [queryParams]);

    return <SourceEditor text={text} readonly={true} wordWrap={true} mode="xml" />;
};