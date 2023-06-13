###############################################
# FILE: moneyAttributes.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 15/May/01 21:06:41
# MODIFIED:
###############################################

@PATH _ROOT _paragraph _sentence

@POST
	S("value") = N("value",1);
	S("type") = N("$text",3);
	single();

@RULES
_money <-
	_money [s]	### (1)
	in [s]	### (2)
	_moneyType [s]	### (3)
	@@

_money <-
	_money [s]	### (1)
	_det [s]	### (2)
	share [s]	### (3)
	@@

@POST
	S("value") = N("value",4);
	S("type") = N("$text",2);
	single();

@RULES
_money <-
    _det [s]	### (1)
    total [s]	### (2)
    _prep [s]	### (3)
    _money [s]	### (4)
    @@

@POST
	S("value") = N("value",2);
	S("type") = N("compare",1);
	single();

@RULES
_money <-
	_compare [s]	### (1)
	_money [s]	### (2)
	@@