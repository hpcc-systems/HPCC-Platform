ServiceOutRecord := RECORD
    string authenticated {XPATH('authenticated')};
END;

output(HTTPCALL('secret:basicsecret','GET', 'application/json', ServiceOutRecord, XPATH('/'), LOG));
