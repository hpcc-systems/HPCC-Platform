###############################################
# FILE: phrases.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 10/Nov/00 15:26:57
# MODIFIED:
###############################################

@PATH _ROOT _paragraph _sentence

@POST
	pncopyvars(1);
	single();
	
@RULES
_company <-
    _company [s]				### (1)
    _companyMarker [s optional]	### (2)
    \( [s]						### (3)
    _xALPHA [s]					### (4)
    \) [s]						### (5)
    @@

@POST
	N("object",3) = makeconcept(N("object",5),"officer");
	addstrval(N("object",3),"title",N("$text",3));
	addstrval(N("object",3),"name",N("$text",1));
	S("object") = N("object",5);
	S("normal") = N("normal",5);
	single();
@RULES
_company <-
    _xWILD [min=1 max=2 match=(_xCAP) gp=_name]	### (1)
    \, [s]										### (2)
    _officer [s]								### (3)
    _prep [s]									### (4)
    _company [s]								### (5)
    \, [s]										### (6)
    @@

@POST
	N("object",1) = makeconcept(N("object",4),"location");
	addstrval(N("object",1),"city",N("$text",1));
	pncopyvars();
	single();
@RULES
_company <-
    _city [s]		### (1)
    \- [s]			### (2)
    based [s]		### (3)
    _company [s]	### (4)
    @@

@POST
	N("object",1) = makeconcept(N("object",5),"founded");
	addstrval(N("object",1),"year",N("$text",3));
	S("object") = N("object");
	S("normal") = N("normal");
	single();
@RULES
_company <-
    _found [s]		### (1)
    _prep [s]		### (2)
    _year [s]		### (3)
    _conj [s]		### (4)
    _company [s]	### (5)
    @@

@POST
	N("object",1) = makeconcept(N("object",1),"location");
	addstrval(N("object",1),"state",N("$text",4));
	pncopyvars(1);
	single();
@RULES
_company <-
    _company [s]		### (1)
    _companyMarker [s]	### (2)
    _prep [s]			### (3)
    _state [s]			### (4)
    @@

@POST
	pncopyvars(1);
	single();	
@RULES
_company <-
    _company [s]		### (1)
    _companyMarker [s]	### (2)
    @@