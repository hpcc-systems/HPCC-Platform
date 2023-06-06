###############################################
# FILE: money.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 15/May/01 21:06:41
# MODIFIED:
###############################################

@PATH _ROOT _paragraph _sentence

@POST
	S("value") = N("value",1) * 1000000;
	single();
@RULES
_money <-
    _money [s]	### (1)
    million [s]	### (2)
    dollars [s optional] ### (3)
    @@

@POST
	S("value") = num(N("$text",1)) * 1000000;
	single();
@RULES
_money <-
    _xNUM [s]	### (1)
    million [s]	### (2)
    dollars [s]	### (3)
    @@
	
