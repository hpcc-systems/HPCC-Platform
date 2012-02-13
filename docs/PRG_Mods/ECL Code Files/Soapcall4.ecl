//
//  Example code - use without restriction.  
//
RoxieIP := 'http://192.168.11.130:8002/WsEcl/soap/query/myroxie/soapcall3.1';			//Roxie
svc     := 'soapcall3.1';

InRec := RECORD
	STRING UIDstr{XPATH('UIDstr')}; 
	STRING LeftInStr{XPATH('LeftInStr')};
	STRING RightInStr{XPATH('RightInStr')};
END;
InDS := DATASET([
			{'1','the quick brown fox jumped over the lazy red dog','quick fox red dog'},
			{'2','the quick brown fox jumped over the lazy red dog','quick fox black dog'},
			{'3','george of the jungle lives here','fox black dog'},
			{'4','fred and wilma flintstone','fred flintstone'},
			{'5','yomama comeonah','brake chill'}
								],InRec);

RS := SOAPCALL(InDS,
							 RoxieIP,
               svc,
							 InRec,
							 TRANSFORM(LEFT),
							 DATASET({STRING UIDval{XPATH('uid')},
							         BOOLEAN CompareResult{XPATH('res')}}));

OUTPUT(RS);
