###############################################
# FILE: anaphoraEvent.pat
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
	S("sentence object") = prev(X("object"));
	if (!N("action"))
		fail();
	
	# LOOP BACK THROUGH SENTENCES
	while (!S("exit") && S("sentence object"))
		{
		N("object") = down(S("sentence object"));
		
		# LOOP THROUGH OBJECTS IN SENTENCE	
		while (!S("exit") && N("object"))
			{
			if (strval(N("object"),"action") == N("action"))
				S("exit") = 1;
			else
				N("object") = next(N("object"));
			}
			
		S("sentence object") = prev(S("sentence object"));
		}

	if (!N("object"))
		{
		"anaphoraEvent.txt" << "Failed: " << phrasetext() << "\n";
		fail();
		}

@POST
	N("anaphora") = N("object");
	S("object") = N("object");
	S("normal") = strval(N("object"),"normal");
	"anaphoraEvent.txt" << "Anaphora:  " << phrasetext() << "\n";
	"anaphoraEvent.txt" << "    from:  " << X("$text") << "\n";
	"anaphoraEvent.txt" << "  Object:  " << conceptname(S("object")) << "\n";
	"anaphoraEvent.txt" << "  Action:  " << N("action") << "\n";
	"anaphoraEvent.txt" << "\n";
	single();
	
@RULES
_eventAnaphora <-
    _anaphora [s]	### (1)
    @@