###############################################
# FILE: lookup.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 04/Dec/00 23:25:19
# MODIFIED:
###############################################

@PATH _ROOT _paragraph _sentence

@CHECK
	S("phrase") = phrasetext();

	"lookup.txt" << "Trying: " << S("phrase") << "\n";
	S("object") = findhierconcept(S("phrase"),G("companies"));
	if (!S("object"))
		fail();

@POST
	S("normal") = conceptname(S("object"));
	if (findattr(S("object"),"synonym"))
		{
		S("object") = up(S("object"));
		S("normal") = conceptname(S("object"));
		}
	"lookup.txt" << "\tFound: " << S("normal") << "\n";
	single();

@RULES

_company <-
    _xALPHA [s]	### (1)
    _xALPHA [s]	### (2)
    _xALPHA [s]	### (3)
    _xALPHA [s]	### (4)
    @@

_company <-
    _xALPHA [s]	### (1)
    _xALPHA [s]	### (2)
    _xALPHA [s]	### (3)
    @@
	
_company <-
    _xALPHA [s]	### (1)
    _xALPHA [s]	### (2)
    @@
	
_company <-
    _xALPHA [s] ### (1)
    @@