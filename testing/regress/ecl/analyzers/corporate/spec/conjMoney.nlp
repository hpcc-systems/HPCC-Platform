###############################################
# FILE: conjMoney.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 10/Nov/00 15:26:53
# MODIFIED:
###############################################

@PATH _ROOT _paragraph _sentence

@POST
	S("conj count") = 0;
	
	S("conj value")[S("conj count")] = N("value",1);
	S("conj type")[S("conj count")++] = N("type",1);
	
	S("conj value")[S("conj count")] = N("value",3);
	S("conj type")[S("conj count")++] = N("type",3);
	
	single();
	
@RULES

_money <-
    _money [s]	### (1)
    _conj [s plus] ### (2)
    _money [s]	### (3)
    @@