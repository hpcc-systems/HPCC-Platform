import * as React from "react";
import { Pivot, PivotItem } from "@fluentui/react";
import { join, scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import * as ESPQuery from "src/ESPQuery";
import * as WsTopology from "src/WsTopology";
import { useWorkunitResults } from "../hooks/workunit";
import { IFrame } from "./IFrame";
import { pushUrl } from "../util/history";
import { XMLSourceEditor } from "./SourceEditor";

const logger = scopedLogger("src-react/components/QueryTests.tsx");

const buildFrameUrl = (url, suffix) => {
    const urlParts = url.split("&src=");
    const src = decodeURIComponent(urlParts[1]);
    return urlParts[0] + "&src=" + encodeURIComponent(join(src, suffix));
};

const frameStyle = { position: "absolute", zIndex: 0, padding: 4, overflow: "none", inset: "44px 0 4px 0" } as React.CSSProperties;

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

        WsTopology.GetWsEclIFrameURL("forms/soap").then(url => setSoapUrl(buildFrameUrl(url, `${querySet}/${queryId}`)));

        WsTopology.GetWsEclIFrameURL("forms/ecl").then(url => setFormUrl(buildFrameUrl(url, `${querySet}/${queryId}`)));

        WsTopology.GetWsEclIFrameURL("forms/json").then(url => setJsonUrl(buildFrameUrl(url, `${querySet}/${queryId}`)));

        WsTopology.GetWsEclIFrameURL("links").then(url => setLinksUrl(buildFrameUrl(url, `${querySet}/${queryId}`)));

        WsTopology.GetWsEclIFrameURL("definitions").then(url => {
            setWsdlUrl(buildFrameUrl(url, `${querySet}/${queryId}/main/${queryId}.wsdl?display`));
            setRequestSchemaUrl(buildFrameUrl(url, `${querySet}/${queryId}/main/${queryId}.xsd?display`));
            setParameterXmlUrl(buildFrameUrl(url, `${querySet}/${queryId}/resource/soap/${queryId}.xml?display`));
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

        WsTopology.GetWsEclIFrameURL("example/request").then(url => setExampleRequestUrl(buildFrameUrl(url, `${querySet}/${queryId}?display`)));

        WsTopology.GetWsEclIFrameURL("example/response").then(url => setExampleResponseUrl(buildFrameUrl(url, `${querySet}/${queryId}?display`)));
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

    return <Pivot
        overflowBehavior="menu" style={{ height: "100%" }} selectedKey={tab}
        onLinkClick={evt => {
            pushUrl(`/queries/${querySet}/${queryId}/testPages/${evt.props.itemKey}`);
        }}
    >
        <PivotItem headerText={nlsHPCC.Form} itemKey="form" style={frameStyle}>
            <IFrame src={formUrl} />
        </PivotItem>
        <PivotItem headerText={nlsHPCC.SOAP} itemKey="soap" style={frameStyle} >
            <IFrame src={soapUrl} />
        </PivotItem>
        <PivotItem headerText={nlsHPCC.JSON} itemKey="json" style={frameStyle}>
            <IFrame src={jsonUrl} />
        </PivotItem>
        <PivotItem headerText={nlsHPCC.WSDL} itemKey="wsdl" style={frameStyle}>
            <IFrame src={wsdlUrl} />
        </PivotItem>
        <PivotItem headerText={nlsHPCC.RequestSchema} itemKey="requestSchema" style={frameStyle}>
            <IFrame src={requestSchemaUrl} />
        </PivotItem>
        <PivotItem headerText={nlsHPCC.ResponseSchema} itemKey="responseSchema" style={frameStyle}>
            {responseSchemas.map((schema, idx) => <XMLSourceEditor key={`responseSchema_${idx}`} text={schema} readonly={true} />)}
        </PivotItem>
        <PivotItem headerText={nlsHPCC.SampleRequest} itemKey="sampleRequest" style={frameStyle}>
            <IFrame src={exampleRequestUrl} />
        </PivotItem>
        <PivotItem headerText={nlsHPCC.SampleResponse} itemKey="sampleResponse" style={frameStyle}>
            <IFrame src={exampleResponseUrl} />
        </PivotItem>
        <PivotItem headerText={nlsHPCC.ParameterXML} itemKey="parameterXml" style={frameStyle}>
            <IFrame src={parameterXmlUrl} />
        </PivotItem>
        <PivotItem headerText={nlsHPCC.Links} itemKey="links" style={frameStyle}>
            <IFrame src={linksUrl} />
        </PivotItem>
    </Pivot>;
};
