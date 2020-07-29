ServiceOutRecord := RECORD
    string authenticated {XPATH('authenticated')};
END;

output(HTTPCALL('secret:urlsecret','GET', 'application/json', ServiceOutRecord, XPATH('/'), LOG), NAMED('URLSecret'));
output(HTTPCALL('secret:basicsecret','GET', 'application/json', ServiceOutRecord, XPATH('/'), LOG), NAMED('CredentialsSecret'));

//WIP needs debugging
//output(HTTPCALL('secret:certsecret','GET', 'text/xml', ServiceOutRecord, LOG), NAMED('ClientCertificateSecret'));
