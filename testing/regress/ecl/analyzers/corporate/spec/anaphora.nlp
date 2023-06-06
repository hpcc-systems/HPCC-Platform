###############################################
# FILE: anaphora.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 15/May/01 22:36:05
# MODIFIED:
###############################################

@PATH _ROOT _paragraph _sentence

@RULES
_anaphora <-
	_pro [s]	### (1)
	@@

@POST
	pncopyvars();
	single();
	
@RULES

_anaphora <-
    _det [s optional] ### (1)
    _adj [s star] ### (2)
	_buyEvent [s]	### (3)
	@@