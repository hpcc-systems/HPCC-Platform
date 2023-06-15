###############################################
# FILE: phrases.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 10/Nov/00 15:26:57
# MODIFIED:
###############################################

@PATH _ROOT _paragraph _sentence
	
@POST
	if (!N("normal"))
		N("normal") = N("$text");
	N("object") = makeconcept(X("object"),N("$text"));
	addstrval(N("object"),"type","company");
	addstrval(N("object"),"normal",N("normal"));

@RULES
_xNIL <-
    _company [s]	### (1)
    @@
