import * as React from "react";
import { SelectTabData, SelectTabEvent, Tab, TabList } from "@fluentui/react-components";
import { scopedLogger } from "@hpcc-js/util";
import { makeStyles } from "@fluentui/react-components";
import { SizeMe } from "../layouts/SizeMe";
import { pivotItemStyle } from "../layouts/pivot";
import { useBuildInfo } from "../hooks/platform";
import { pushUrl } from "../util/history";
import { DESDLBindingSummary } from "./DESDLBindingSummary";
import { DESDLBindingMethods } from "./DESDLBindingMethods";
import { XMLSourceEditor } from "./SourceEditor";
import * as WsESDLConfig from "src/WsESDLConfig";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/ESDLBindingDetails.tsx");

const useStyles = makeStyles({
    container: {
        height: "100%",
        position: "relative"
    }
});

interface DESDLBindingDetailsProps {
    name: string,
    tab?: string
}

export const DESDLBindingDetails: React.FunctionComponent<DESDLBindingDetailsProps> = ({
    name,
    tab = "summary"
}) => {

    const [, { opsCategory }] = useBuildInfo();

    const [binding, setBinding] = React.useState<any>();

    React.useEffect(() => {
        WsESDLConfig.GetESDLBinding({ request: { EsdlBindingId: name, IncludeInterfaceDefinition: true, ReportMethodsAvailable: true } })
            .then(({ GetESDLBindingResponse }) => {
                setBinding(GetESDLBindingResponse);
            })
            .catch(err => logger.error(err))
            ;
    }, [name]);

    const onTabSelect = React.useCallback((_: SelectTabEvent, data: SelectTabData) => {
        pushUrl(`/${opsCategory}/desdl/bindings/${name}/${data.value as string}`);
    }, [name, opsCategory]);

    const styles = useStyles();

    return <SizeMe>{({ size }) =>
        <div className={styles.container}>
            <TabList selectedValue={tab} onTabSelect={onTabSelect} size="medium">
                <Tab value="summary">{nlsHPCC.Summary}</Tab>
                <Tab value="configuration">{nlsHPCC.title_BindingConfiguration}</Tab>
                <Tab value="definition">{nlsHPCC.title_BindingDefinition}</Tab>
            </TabList>
            {tab === "summary" &&
                <div style={pivotItemStyle(size)}>
                    <DESDLBindingSummary processName={binding?.EspProcName} serviceName={binding?.ServiceName} port={binding?.EspPort} bindingName={name} />
                </div>
            }
            {tab === "configuration" &&
                <div style={pivotItemStyle(size, 0)}>
                    <DESDLBindingMethods name={name} />
                </div>
            }
            {tab === "definition" &&
                <div style={pivotItemStyle(size, 0)}>
                    <XMLSourceEditor text={binding?.ESDLBinding?.Definition?.Interface} readonly={true} />
                </div>
            }
        </div>
    }</SizeMe>;

};