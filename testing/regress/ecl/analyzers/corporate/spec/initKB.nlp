###############################################
# FILE: initKB.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 14/Nov/00 18:13:17
# MODIFIED:
###############################################

@CODE
	
G("corporate") = findconcept(findroot(),"corporate");
if (!G("corporate"))
	G("corporate") = makeconcept(findroot(),"corporate");

G("parse") = findconcept(G("corporate"),"parse");
if (G("parse"))
	rmchildren(G("parse"));
else
	G("parse") = makeconcept(G("corporate"),"parse");
	
G("companies") = findconcept(G("corporate"),"companies");
G("section number") = 0;

# IF COMPANIES NOT IN KB, ADD THEM HERE.	# 06/21/07 AM.
if (G("companies"))
	exitpass();
G("companies") = getconcept(G("corporate"),"companies");

L("co") = getconcept(G("companies"),"TAI");
getconcept(L("co"),"Text Analysis International");

L("co") = getconcept(G("companies"),"AMRP");
getconcept(L("co"),"American Medical Records Processing");

@@CODE