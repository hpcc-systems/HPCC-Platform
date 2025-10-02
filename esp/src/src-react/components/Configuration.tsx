import * as React from "react";
import { scopedLogger } from "@hpcc-js/util";
import { XMLSourceEditor } from "./SourceEditor";
import * as ESPRequest from "src/ESPRequest";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/Configuration.tsx");

const configErrors: RegExp[] = [/Page Not Found/i, /Error/i];

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
            if (configErrors.some(reg => reg.test(response))) {
                logger.info(nlsHPCC.ErrorLoadingConfigurationFile);
                setConfigXml(nlsHPCC.ErrorLoadingConfigurationFile);
            } else {
                setConfigXml(response);
            }
        }).catch(err => {
            logger.warning(err);
        });
    }, []);

    return <XMLSourceEditor text={configXml} readonly={true} />;

};
