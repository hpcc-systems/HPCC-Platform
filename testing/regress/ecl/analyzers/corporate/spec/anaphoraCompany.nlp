###############################################
# FILE: anaphoraCompany.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 07/Dec/00 13:31:53
# MODIFIED:
###############################################

@PATH _ROOT _paragraph _sentence

@CHECK
## FIND LAST MATCHING OBJECT
	S("exit") = 0;
	N("anaphora") = 0;
	S("sentence object") = X("object");
	if (N("action"))
		fail();
	
	# LOOP BACK THROUGH SENTENCES
	while (!S("exit") && S("sentence object"))
		{
		N("object") = down(S("sentence object"));
		
		# LOOP THROUGH OBJECTS IN SENTENCE	
		while (!S("exit") && N("object"))
			{
			if (!strval(N("object"),"action"))
				S("exit") = 1;
			else
				N("object") = next(N("object"));
			}
			
		S("sentence object") = prev(S("sentence object"));
		}
	if (!N("object"))
		{
		"anaphora.txt" << "Failed!" << "\n";
		fail();
		}

@POST
	N("anaphora") = N("object");
	S("object") = N("object");
	S("normal") = strval(N("object"),"normal");
	"anaphora.txt" << "Anaphora:  " << phrasetext() << "\n";
	"anaphora.txt" << "    from:  " << X("$text") << "\n";
	"anaphora.txt" << "  Object:  " << conceptname(S("object")) << "\n";
	if (N("type"))
		"anaphora.txt" << "    Type:  " << N("type") << "\n";
	"anaphora.txt" << "\n";
	single();
	
@RULES
_company <-
    _anaphora [s]	### (1)
    @@