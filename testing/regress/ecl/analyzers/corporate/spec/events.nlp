###############################################
# FILE: events.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 10/Nov/00 15:26:57
# MODIFIED:
###############################################

@PATH _ROOT _paragraph _sentence

@POST
	S("stock") = makeconcept(N("object",1),"stock");
	addstrval(S("stock"),"action",N("$text",4));
	addstrval(S("stock"),"percent",N("$text",5));
	addnumval(S("stock"),"from",num(N("value",7)));
	addnumval(S("stock"),"to",num(N("value",9)));
	single();

@RULES
_event <-
    _company [s]	### (1)
    stock [s]	### (2)
    _be [s]	### (3)
    _direction [s]	### (4)
    _percent [s]	### (5)
    from [s]	### (6)
    _money [s]	### (7)
    _to [s]	### (8)
    _money [s]	### (9)
    @@

@POST
	S("action") = makeconcept(X("object"),N("action",3));
	addstrval(S("action"),"action","buy");
	S("company1") = makeconcept(S("action"),"company1");
	S("company2") = makeconcept(S("action"),"company2");
	S("amount") = makeconcept(S("action"),"amount");
	addstrval(S("company1"),"name",N("normal",1));

	# COMPANY2 CONJUNCTION	
	if (N("conj count",4))
		{
		S("conj count") = 0;
		while (S("conj count") < N("conj count",4))
			addstrval(S("company2"),"name",N("conj",4)[S("conj count")++]);
		}
	else
		addstrval(S("company2"),"name",N("normal",4));

	# MONEY CONJUNCTION
	if (N("conj count",6))
		{
		S("conj count") = 0;
		while (S("conj count") < N("conj count",6))
			addnumval(S("amount"),N("conj type",6)[S("conj count")],
				num(N("conj value",6)[S("conj count")++]) );
		}
	else
		addnumval(S("amount"),N("type",6),N("value",6));
		
	single();

@RULES
_event <-
    _company [s]	### (1)
    _have [s optional] ### (2)
    _xWILD [s matches=(_buy _acquire)]	### (3)
    _company [s]	### (4)
    for [s]	### (5)
    _money [s]	### (6)
    @@