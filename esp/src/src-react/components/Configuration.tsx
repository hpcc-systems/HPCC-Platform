import * as React from "react";
import { XMLSourceEditor } from "./SourceEditor";
import * as ESPRequest from "../../src/ESPRequest";

interface ConfigurationProps {
}

export const Configuration: React.FunctionComponent<ConfigurationProps> = ({
}) => {

    const [configXml, setConfigXml] = React.useState("");

    React.useEffect(() => {
        ESPRequest.send("main", "", {
            request: {
                config_: "",
                PlainText: "yes"
            },
            handleAs: "text"
        }).then(response => {
            setConfigXml(response);
        });
    }, []);

    return <XMLSourceEditor text={configXml} readonly={true} />;

};