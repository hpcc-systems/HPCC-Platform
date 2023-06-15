###############################################
# FILE: sentences.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 14/Nov/00 18:07:58
# MODIFIED:
###############################################

@CODE
# Sentence counter.	# 05/17/01 AM.
G("sentence count") = 0;

@NODES _ROOT

@POST
	++G("sentence count");	# 05/17/01 AM.
	S("name") = "sentence" + str(G("sentence count"));	# 05/17/01 AM.
	S("object") = makeconcept(G("parse"),S("name"));	# 05/17/01 AM.
	addstrval(S("object"),"text",N("$text",1));
	single();

@RULES

_sentence [unsealed] <-
    _xWILD [s plus fails=(\. \? \! _paragraphSeparator)]	### (1)
    _xWILD [s one matches=(\. \? \!)] ### (2)
    @@

_sentence [unsealed] <-
    _xWILD [s plus fails=(_paragraphSeparator)]	### (1)
    @@
