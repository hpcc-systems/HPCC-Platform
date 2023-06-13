###############################################
# FILE: events.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 10/Nov/00 15:26:57
# MODIFIED:
###############################################

@PATH _ROOT _paragraph _sentence

@POST
	N("date",5) = makeconcept(N("action",5),"date");
	addstrval(N("date",5),"year",N("$text",2));
@RULES
_xNIL <-
    _prep [s]	### (1)
    _year [s]	### (2)
    alone [s]	### (3)
    _conj [s]	### (4)
    _event [s]	### (5)
    @@