ServiceOutRecord := RECORD
    string authenticated {XPATH('authenticated')};
END;


//call out using our http-connect-basicsecret.. but get it directly from the ecl vault

output(HTTPCALL('secret:my-ecl-vault:basicsecret','GET', 'application/json', ServiceOutRecord, XPATH('/'), LOG));
