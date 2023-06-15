###############################################
# FILE: events2.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 10/Nov/00 15:26:57
# MODIFIED:
###############################################

@PATH _ROOT _paragraph _sentence

@POST
	S("comment") = makeconcept(N("object",1),"comment");
	addstrval(S("comment"),"position",N("$text",9));
	addstrval(S("comment"),"commentor",N("$text",10));
	single();

@RULES
_event <-
	_eventAnaphora [s]						### (1)
    _prep [s]								### (2)
    _det [s]								### (3)
    _xWILD [s plus except=(_companyMarker)]	### (4)
    _companyMarker [s]						### (5)
    for [s]									### (6)
    _money [s]								### (7)
    _be [s]									### (8)
    _position [s]							### (9)
    _commentor [s]							### (10)
    @@

@POST
	S("comment") = makeconcept(N("object",1),"comment");
	addstrval(S("comment"),"position",N("$text",6));
	addstrval(S("comment"),"degree",N("$text",5));
	addstrval(S("comment"),"field",N("$text",11));
	single();

@RULES
_event <-
	_eventAnaphora [s]				### (1)
    _conj [s optional]				### (2)
    _company [s]					### (3)
    _have [s]						### (4)
    _adv [s]						### (5)
    strengthened [s]				### (6)
    _company [s]					### (7)
    hold [s]						### (8)
    on [s]							### (9)
    _det [s]						### (10)
    _xWILD [s plus except=(_field)]	### (11)
    _field [s]						### (12)
    @@