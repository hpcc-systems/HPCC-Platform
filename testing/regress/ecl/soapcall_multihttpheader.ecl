string TargetIP := '.' : stored('TargetIP');
string storedHeader := 'StoredHeaderDefault' : stored('storedHeader');

httpEchoServiceResponseRecord :=
    RECORD
        string method{xpath('Method')};
        string path{xpath('UrlPath')};
        string parameters{xpath('UrlParameters')};
        set of string headers{xpath('Headers/Header')};
        string content{xpath('Content')};
    END;

string TargetURL := 'http://' + TargetIP + ':8010/WsSmc/HttpEcho?name=doe,joe&number=1';


httpEchoServiceRequestRecord :=
    RECORD
       string Name{xpath('Name')} := 'Doe, Joe',
       unsigned id{xpath('ADL')} := 999999,
       real8 score := 88.88,
    END;

string constHeader := 'constHeaderValue';

soapcallResult := SOAPCALL(TargetURL, 'HttpEcho', httpEchoServiceRequestRecord, DATASET(httpEchoServiceResponseRecord), LITERAL, xpath('HttpEchoResponse'), httpheader('StoredHeader', storedHeader), httpheader('literalHeader', 'literalHeaderValue'), httpheader('constHeader', constHeader));

output(soapcallResult, named('soapcallResult'));


//test proxyaddress functionality by using an invalid targetUrl, but a valid proxyaddress.  HTTP Host header will be wrong, but should still work fine as it's ignored by ESP.
string HostURL := 'http://1.1.1.1:9999/WsSmc/HttpEcho?name=doe,joe&number=1';
string TargetProxy := 'http://' + TargetIP + ':8010';

proxyResult := SOAPCALL(HostURL, 'HttpEcho', httpEchoServiceRequestRecord, DATASET(httpEchoServiceResponseRecord), LITERAL, xpath('HttpEchoResponse'), proxyAddress(TargetProxy), httpheader('StoredHeader', storedHeader), httpheader('literalHeader', 'literalHeaderValue'), httpheader('constHeader', constHeader));

output(proxyResult, named('proxyResult'));
