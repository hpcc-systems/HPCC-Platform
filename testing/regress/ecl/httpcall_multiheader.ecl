string TargetIP := '.' : stored('TargetIP');
string storedHeader := 'StoredHeaderDefault' : stored('StoredHeader');


httpEchoServiceResponseRecord :=
    RECORD
        string method{xpath('Method')};
        string path{xpath('UrlPath')};
        string parameters{xpath('UrlParameters')};
        set of string headers{xpath('Headers/Header')};
        string content{xpath('Content')};
    END;

string TargetURL := 'http://' + TargetIP + ':8010/WsSmc/HttpEcho?name=doe,joe&number=1';

string constHeader := 'constHeaderValue';

httpcallResult := HTTPCALL(TargetURL,'GET', 'text/xml', httpEchoServiceResponseRecord, xpath('Envelope/Body/HttpEchoResponse'),httpheader('literalHeader','literalValue'), httpheader('constHeader','constHeaderValue'), httpheader('storedHeader', storedHeader), httpheader('HPCC-Global-Id','9876543210'), httpheader('HPCC-Caller-Id','http111'));
output(httpcallResult, named('httpcallResult'));

//test proxyaddress functionality by using an invalid targetUrl, but a valid proxyaddress.  HTTP Host header will be wrong, but should still work fine as it's ignored by ESP.
string hostURL := 'http://1.1.1.1:9999/WsSmc/HttpEcho?name=doe,joe&number=1';
string targetProxy := 'http://' + TargetIP + ':8010';

proxyResult := HTTPCALL(hostURL,'GET', 'text/xml', httpEchoServiceResponseRecord, xpath('Envelope/Body/HttpEchoResponse'), proxyaddress(targetProxy), httpheader('literalHeader','literalValue'), httpheader('constHeader','constHeaderValue'), httpheader('storedHeader', storedHeader), httpheader('HPCC-Global-Id','9876543210'), httpheader('HPCC-Caller-Id','http222'));

output(proxyResult, named('proxyResult'));
