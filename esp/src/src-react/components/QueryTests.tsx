import * as React from "react";
import { Pivot, PivotItem } from "@fluentui/react";
import { join, scopedLogger } from "@hpcc-js/util";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import * as ESPQuery from "src/ESPQuery";
import * as WsTopology from "src/WsTopology";
import { useWorkunitResults } from "../hooks/workunit";
import { pivotItemStyle } from "../layouts/pivot";
import { IFrame } from "./IFrame";
import { pushUrl } from "../util/history";
import { XMLSourceEditor } from "./SourceEditor";

const logger = scopedLogger("src-react/components/QueryTests.tsx");

interface QueryTestsProps {
    querySet: string;
    queryId: string;
    tab?: string;
}

export const QueryTests: React.FunctionComponent<QueryTestsProps> = ({
    querySet,
    queryId,
    tab = "form"
}) => {

    const [query, setQuery] = React.useState<any>();

    const [wuid, setWuid] = React.useState("");
    const [wuResults] = useWorkunitResults(wuid);
    const [resultNames, setResultNames] = React.useState([]);

    const [soapUrl, setSoapUrl] = React.useState("");
    const [jsonUrl, setJsonUrl] = React.useState("");
    const [wsdlUrl, setWsdlUrl] = React.useState("");
    const [requestSchemaUrl, setRequestSchemaUrl] = React.useState("");
    const [responseSchemas, setResponseSchemas] = React.useState([]);
    const [exampleRequestUrl, setExampleRequestUrl] = React.useState("");
    const [exampleResponseUrl, setExampleResponseUrl] = React.useState("");
    const [parameterXmlUrl, setParameterXmlUrl] = React.useState("");
    const [formUrl, setFormUrl] = React.useState("");
    const [linksUrl, setLinksUrl] = React.useState("");

    React.useEffect(() => {
        setQuery(ESPQuery.Get(querySet, queryId));
        WsTopology.GetWsEclIFrameURL("forms/soap").then(response => {
            setSoapUrl(join(response, encodeURIComponent(`WsEcl/forms/soap/${querySet}/${queryId}`)));
        });
        WsTopology.GetWsEclIFrameURL("forms/ecl").then(response => {
            setFormUrl(join(response, encodeURIComponent(`WsEcl/forms/ecl/${querySet}/${queryId}`)));
        });
        WsTopology.GetWsEclIFrameURL("forms/json").then(response => {
            setJsonUrl(join(response, encodeURIComponent(`WsEcl/forms/json${querySet}/${queryId}`)));
        });
        WsTopology.GetWsEclIFrameURL("links").then(response => {
            setLinksUrl(join(response, encodeURIComponent(`WsEcl/links/query/${querySet}/${queryId}`)));
        });
        WsTopology.GetWsEclIFrameURL("definitions").then(response => {
            setWsdlUrl(join(response, encodeURIComponent(`/WsEcl/definitions/query/${querySet}/${queryId}/main/${queryId}.wsdl`)));
            setRequestSchemaUrl(join(response, encodeURIComponent(`/WsEcl/definitions/query/${querySet}/${queryId}/main/${queryId}.xsd`)));
            setParameterXmlUrl(join(response, encodeURIComponent(`/WsEcl/definitions/query/${querySet}/${queryId}/resource/soap/${queryId}.xml`)));
        });
        const requests = resultNames.map(name => {
            const url = `/WsEcl/definitions/query/${querySet}/${queryId}/result/${name}.xsd`;
            return fetch(url)
                .then(response => response.text())
                .catch(err => logger.error(err));
        });
        Promise.all(requests)
            .then(schemas => {
                logger.debug(schemas);
                setResponseSchemas(schemas);
            })
            .catch(err => logger.error(err));
        WsTopology.GetWsEclIFrameURL("example/request").then(response => {
            setExampleRequestUrl(join(response, encodeURIComponent(`/WsEcl/example/request/query/${querySet}/${queryId}`)));
        });
        WsTopology.GetWsEclIFrameURL("example/response").then(response => {
            setExampleResponseUrl(join(response, encodeURIComponent(`/WsEcl/example/response/query/${querySet}/${queryId}`)));
        });
    }, [setQuery, queryId, querySet, resultNames]);

    React.useEffect(() => {
        query?.getDetails()
            .then(({ WUQueryDetailsResponse }) => {
                setWuid(WUQueryDetailsResponse.Wuid);
            })
            .catch(err => logger.error(err));
    }, [query]);

    React.useEffect(() => {
        const names = [];
        wuResults.forEach(result => {
            names.push(result.Name.replace(/ /g, "_"));
        });
        setResultNames(names);
    }, [wuResults]);

    return <SizeMe monitorHeight>{({ size }) =>
        <Pivot
            overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab}
            onLinkClick={evt => {
                pushUrl(`/queries/${querySet}/${queryId}/testPages/${evt.props.itemKey}`);
            }}
        >
            <PivotItem headerText={nlsHPCC.Form} itemKey="form" style={pivotItemStyle(size, 0)}>
                <IFrame src={formUrl} height="99%" />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.SOAP} itemKey="soap" style={pivotItemStyle(size)} >
                <IFrame src={soapUrl} height="99%" />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.JSON} itemKey="json" style={pivotItemStyle(size, 0)}>
                <IFrame src={jsonUrl} height="99%" />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.WSDL} itemKey="wsdl" style={pivotItemStyle(size, 0)}>
                <IFrame src={wsdlUrl} height="99%" />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.RequestSchema} itemKey="requestSchema" style={pivotItemStyle(size, 0)}>
                <IFrame src={requestSchemaUrl} height="99%" />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.ResponseSchema} itemKey="responseSchema" style={pivotItemStyle(size, 0)}>
                {responseSchemas.map((schema, idx) => <XMLSourceEditor key={`responseSchema_${idx}`} text={schema} readonly={true} />)}
            </PivotItem>
            <PivotItem headerText={nlsHPCC.SampleRequest} itemKey="sampleRequest" style={pivotItemStyle(size, 0)}>
                <IFrame src={exampleRequestUrl} height="99%" />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.SampleResponse} itemKey="sampleResponse" style={pivotItemStyle(size, 0)}>
                <IFrame src={exampleResponseUrl} height="99%" />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.ParameterXML} itemKey="parameterXml" style={pivotItemStyle(size, 0)}>
                <IFrame src={parameterXmlUrl} height="99%" />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Links} itemKey="links" style={pivotItemStyle(size, 0)}>
                <IFrame src={linksUrl} height="99%" />
            </PivotItem>
        </Pivot>
    }</SizeMe>;
};
