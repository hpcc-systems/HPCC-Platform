ServiceQueryRequest := RECORD
  string Type { XPATH('Type') } := 'EclWatch';
  string Name { XPATH('Name') } := '';
END;

HpccServiceRecord := RECORD
  string Name { XPATH('Name') };
  string Type { XPATH('Type') };
  integer2 Port { XPATH('Port') };
  boolean TLSSecure { XPATH('TLSSecure') };
END;


ServiceQueryResponse := RECORD
    dataset(HpccServiceRecord) services { XPATH('Services/Service') };
END;



// Get the list of EclWatch services by calling WsResources/ServiceQueryCall using several different protocols.
//
//


//---------------- HTTP --------------------------------

//Using an HTTP GET URL that returns JSON content:

OUTPUT(HTTPCALL('http://.:8010/WsResources/ServiceQuery.json?Type=EclWatch','GET', 'application/json', ServiceQueryResponse, XPATH('/ServiceQueryResponse'), LOG('http get json')), NAMED('http_get_json'));

//Using a HTTP GET URL that returns XML content:

OUTPUT(HTTPCALL('http://.:8010/WsResources/ServiceQuery.xml?Type=EclWatch','GET', 'text/xml', ServiceQueryResponse, XPATH('/ServiceQueryResponse'), LOG('http get xml')), NAMED('http_get_xml'));

//Using a JSON post

OUTPUT(HTTPCALL('http://.:8010/WsResources','ServiceQueryRequest', ServiceQueryRequest, ServiceQueryResponse, JSON, XPATH('ServiceQueryResponse'), LOG('http post json')), NAMED('http_post_json'));



//Using a form url encoded post
//
// Note: for nested content the following mapping / notation will be used:
// FORMENCODED('dot') ==> a[1].b[2].c=123
// FORMENCODED('bracket') ==> a[1][b][2][c]=123
// FORMENCODED('esp') ==> a.1.b.2.c=123
//


OUTPUT(HTTPCALL('http://.:8010/WsResources/ServiceQuery','', ServiceQueryRequest, ServiceQueryResponse, FORMENCODED('esp'), XPATH('ServiceQueryResponse'), LOG('form encoded post json')), NAMED('form_post'));


//---------------- SOAP --------------------------------

//Using SOAP LITERAL:

OUTPUT(SOAPCALL('http://.:8010/WsResources','ServiceQueryRequest', ServiceQueryRequest, ServiceQueryResponse, LITERAL, XPATH('ServiceQueryResponse'), LOG('soap literal')), NAMED('soap_literal'));

//Using ESP specific SOAP

OUTPUT(SOAPCALL('http://.:8010/WsResources','ServiceQuery', ServiceQueryRequest, ServiceQueryResponse, XPATH('ServiceQueryResponse'), LOG('soap esp')), NAMED('soap_esp'));
