import * as React from "react";
import { Pivot, PivotItem } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { SizeMe } from "react-sizeme";
import { pivotItemStyle } from "../layouts/pivot";
import { pushUrl } from "../util/history";
import { DESDLBindingSummary } from "./DESDLBindingSummary";
import { DESDLBindingMethods } from "./DESDLBindingMethods";
import { XMLSourceEditor } from "./SourceEditor";
import * as WsESDLConfig from "src/WsESDLConfig";
import nlsHPCC from "src/nlsHPCC";

const logger = scopedLogger("src-react/components/ESDLBindingDetails.tsx");

interface DESDLBindingDetailsProps {
    name: string,
    tab?: string
}

export const DESDLBindingDetails: React.FunctionComponent<DESDLBindingDetailsProps> = ({
    name,
    tab = "summary"
}) => {

    const [binding, setBinding] = React.useState<any>();

    React.useEffect(() => {
        WsESDLConfig.GetESDLBinding({ request: { EsdlBindingId: name, IncludeInterfaceDefinition: true, ReportMethodsAvailable: true } })
            .then(({ GetESDLBindingResponse }) => {
                setBinding(GetESDLBindingResponse);
            })
            .catch(err => logger.error(err))
            ;
    }, [name]);

    return <SizeMe monitorHeight>{({ size }) =>
        <Pivot overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab} onLinkClick={evt => pushUrl(`/desdl/bindings/${name}/${evt.props.itemKey}`)}>
            <PivotItem headerText={nlsHPCC.Summary} itemKey="summary" style={pivotItemStyle(size)}>
                <DESDLBindingSummary processName={binding?.EspProcName} serviceName={binding?.ServiceName} port={binding?.EspPort} bindingName={name} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.title_BindingConfiguration} itemKey="configuration" style={pivotItemStyle(size, 0)}>
                <DESDLBindingMethods name={name} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.title_BindingDefinition} itemKey="definition" style={pivotItemStyle(size, 0)}>
                <XMLSourceEditor text={binding?.ESDLBinding?.Definition?.Interface} readonly={true} />
            </PivotItem>
        </Pivot>
    }</SizeMe>;

};