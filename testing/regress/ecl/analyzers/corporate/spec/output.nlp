###############################################
# FILE: output.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 10/Nov/00 15:35:55
# MODIFIED: 07/Dec/00
###############################################

@CODE

G("out") = cbuf();

G("sentence") = down(G("parse"));

while (G("sentence"))
	{
	G("object") = down(G("sentence"));
	G("out") << "-------------------------------------------------------------\n";
	G("out") << "\"" << strwrap(strval(G("sentence"),"text"),60) << "\"\n\n";
	
	while (G("object"))
		{
		G("printed") = 0;
		G("subobject") = down(G("object"));

		if (strval(G("object"),"type") == "company")
			{
			if (G("subobject"))
				G("out") << "Company: " << conceptname(G("object")) << "\n";
			}
		else
			G("out") << "Action: " << conceptname(G("object")) << "\n";
		
		G("last subobject") = " ";
		while (G("subobject"))
			{
			G("attributes") = findattrs(G("subobject"));
			while (G("attributes"))
				{
				G("values") = attrvals(G("attributes"));
				while (G("values"))
					{
					G("out") << "   " << conceptname(G("subobject")) << ": ";
					G("out") << "(" << attrname(G("attributes")) << ") ";
					if (getstrval(G("values")))
						G("out") << getstrval(G("values")) << "\n";
					else
						G("out") << getnumval(G("values")) << "\n";
					G("printed") = 1;
					G("values") = nextval(G("values"));
					}
				G("attributes") = nextattr(G("attributes"));
				}

			if (G("last subobject") != conceptname(G("subobject")))
				{
				G("out") << "\n";
				G("printed") = 0;
				}
			G("last subobject") = conceptname(G("subobject"));
			G("subobject") = next(G("subobject"));
			}
			
		if (G("printed"))
			G("out") << "\n";
			
		G("object") = next(G("object"));
		}
	G("sentence") = next(G("sentence"));
	}

@@CODE