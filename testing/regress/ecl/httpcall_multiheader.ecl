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

httpcallResult := HTTPCALL(TargetURL,'GET', 'text/xml', httpEchoServiceResponseRecord, xpath('Envelope/Body/HttpEchoResponse'),httpheader('literalHeader','literalValue'), httpheader('constHeader','constHeaderValue'), httpheader('storedHeader', storedHeader));

output(httpcallResult);
