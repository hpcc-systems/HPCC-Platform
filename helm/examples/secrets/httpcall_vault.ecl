ServiceOutRecord := RECORD
    string authenticated {XPATH('authenticated')};
END;

//call out using our http-connect-vaultsecret stored in our ecl vault

output(HTTPCALL('secret:vaultsecret','GET', 'application/json', ServiceOutRecord, XPATH('/'), LOG));
