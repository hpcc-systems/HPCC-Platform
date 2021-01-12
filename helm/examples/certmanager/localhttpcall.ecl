responseRecord :=
    RECORD
        string method{xpath('Method')};
        string path{xpath('UrlPath')};
        string parameters{xpath('UrlParameters')};
        set of string headers{xpath('Headers/Header')};
        string content{xpath('Content')};
    END;

//mtls: tells HTTPCALL/SOAPCALL to use mutual TLS, using the local client and CA certificates
//
string hostURL := 'mtls:https://eclservices:8010/WsSmc/HttpEcho?name=doe,joe&number=1';

echoResult := HTTPCALL(hostURL,'GET', 'text/xml', responseRecord, xpath('Envelope/Body/HttpEchoResponse'));
output(echoResult, named('localHttpEchoResult'));
