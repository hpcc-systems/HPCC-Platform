import * as React from "react";
import { Pivot, PivotItem } from "@fluentui/react";
import { scopedLogger } from "@hpcc-js/util";
import { SizeMe } from "react-sizeme";
import nlsHPCC from "src/nlsHPCC";
import * as ESPQuery from "src/ESPQuery";
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
    const [wsdl, setWsdl] = React.useState("");
    const [requestSchema, setRequestSchema] = React.useState("");
    const [responseSchemas, setResponseSchemas] = React.useState([]);
    const [exampleRequest, setExampleRequest] = React.useState("");
    const [exampleResponse, setExampleResponse] = React.useState("");
    const [parameterXml, setParameterXml] = React.useState("");
    const [formUrl, setFormUrl] = React.useState("");
    const [linksUrl, setLinksUrl] = React.useState("");

    React.useEffect(() => {
        setQuery(ESPQuery.Get(querySet, queryId));
        setSoapUrl(`/WsEcl/forms/soap/query/${querySet}/${queryId}`);
        setJsonUrl(`/WsEcl/forms/json/query/${querySet}/${queryId}`);
        fetch(`/WsEcl/definitions/query/${querySet}/${queryId}/main/${queryId}.wsdl`)
            .then(response => response.text())
            .then(content => setWsdl(content))
            .catch(err => logger.error(err));

        fetch(`/WsEcl/definitions/query/${querySet}/${queryId}/main/${queryId}.xsd`)
            .then(response => response.text())
            .then(content => setRequestSchema(content))
            .catch(err => logger.error(err));

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

        fetch(`/WsEcl/example/request/query/${querySet}/${queryId}`)
            .then(response => response.text())
            .then(content => setExampleRequest(content))
            .catch(err => logger.error(err));

        fetch(`/WsEcl/example/response/query/${querySet}/${queryId}`)
            .then(response => response.text())
            .then(content => setExampleResponse(content))
            .catch(err => logger.error(err));

        fetch(`/WsEcl/definitions/query/${querySet}/${queryId}/resource/soap/${queryId}.xml`)
            .then(response => response.text())
            .then(content => setParameterXml(content))
            .catch(err => logger.error(err));

        setFormUrl(`/WsEcl/forms/ecl/query/${querySet}/${queryId}`);
        setLinksUrl(`/WsEcl/links/query/${querySet}/${queryId}`);
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
                <XMLSourceEditor text={wsdl} readonly={true} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.RequestSchema} itemKey="requestSchema" style={pivotItemStyle(size, 0)}>
                <XMLSourceEditor text={requestSchema} readonly={true} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.ResponseSchema} itemKey="responseSchema" style={pivotItemStyle(size, 0)}>
                {responseSchemas.map((schema, idx) => <XMLSourceEditor key={`responseSchema_${idx}`} text={schema} readonly={true} />)}
            </PivotItem>
            <PivotItem headerText={nlsHPCC.SampleRequest} itemKey="sampleRequest" style={pivotItemStyle(size, 0)}>
                <XMLSourceEditor text={exampleRequest} readonly={true} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.SampleResponse} itemKey="sampleResponse" style={pivotItemStyle(size, 0)}>
                <XMLSourceEditor text={exampleResponse} readonly={true} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.ParameterXML} itemKey="parameterXml" style={pivotItemStyle(size, 0)}>
                <XMLSourceEditor text={parameterXml} readonly={true} />
            </PivotItem>
            <PivotItem headerText={nlsHPCC.Links} itemKey="links" style={pivotItemStyle(size, 0)}>
                <IFrame src={linksUrl} height="99%" />
            </PivotItem>
        </Pivot>
    }</SizeMe>;
};
