###############################################
# FILE: conjCompany.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 10/Nov/00 15:26:53
# MODIFIED:
###############################################

@PATH _ROOT _paragraph _sentence

@POST
	S("conj count") = 0;
	S("conj")[S("conj count")++] = N("normal",1);
	S("conj")[S("conj count")++] = N("normal",3);
	S("conj")[S("conj count")++] = N("normal",5);
	single();
	
@RULES

_company <-
    _company [s]	### (1)
    _conj [s plus] ### (2)
    _company [s]	### (3)
    _conj [s plus] ### (4)
    _company [s]	### (5)
    @@