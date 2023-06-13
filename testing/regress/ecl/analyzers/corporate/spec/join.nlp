###############################################
# FILE: join.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 14/Nov/00 18:13:17
# MODIFIED:
###############################################

@NODES _ROOT
	
@RULES

_num <-
    _xNUM [s]	### (1)
    \. [s]		### (2)
    _xNUM [s]	### (3)
    @@
	
_year [base] <-
    \' [s]	### (1)
    _xWILD [s one matches=(90 91 92 93 94 95 96 97 98 99 01 02 03 04 05 06)] ### (2)
    @@
	
_year [base] <-
    _xWILD [s one match=(1972 1999 2000 2001 2002 2003 2004 2005 2006)] ### (1)
    @@

_paragraphSeparator <-
    \n [s min=2 max=0]	### (1)
    @@

@POST
	N("quoted",1) = 1;
	excise(4,4);
	excise(1,1);
@RULES
_xNIL <-
    \` [s]		### (1)
    _xALPHA [s]	### (2)
    \. [s trig]	### (3)
    \' [s]		### (4)
    @@
		
@RULES
_modalNot <-
    _xWILD [s one matches=(hasn havn doesn didn don couldn can)]	### (1)
    \' [s]	### (2)
    t [s]	### (3)
    @@
	
@POST
	N("possessive",1) = 1;
	excise(2,3);
@RULES
_xNIL <-
    _xALPHA [s]		### (1)
    \' [s trig]		### (2)
    s [s optional]	### (3)
    @@


@POST
    L("tmp") = N("$text",2) + "." + N("$text",4);
	S("value") = flt(L("tmp"));
#	S("value") = num(N("$text",2));
	single();
@RULES		
_money <-
    _xWILD [s one matches=(\$ )]		### (1)
    _xNUM [s]							### (2)
    _xWILD [s one matches=( \. \, )]	### (3)
    _xNUM [s]							### (4)
    @@

@POST
	S("value") = num(N("$text"));
	single();
@RULES		
_money <-
    _xWILD [s one matches=(\$ )]	### (1)
    _xNUM [s]	### (2)
    @@

@RULES
_abbrev <-
    _xCAP [s]	### (1)
    \. [s]		### (2)
    _xCAP [s]	### (3)
    \. [s]		### (4)
    @@
	
_alphaNum <-
    _xALPHA [s]	### (1)
    _xNUM [s] ### (2)
    @@
	
_numAlpha <-
    _xNUM [s] ### (1)
    _xALPHA [s] ### (2)
    @@

_number <-
    _xNUM [s]	### (1)
    \, [s]	### (2)
    _xNUM [s]	### (3)
    @@

_percent <-
    _xNUM [s] ### (1)
	\% [s]	### (2)
	@@

@POST
	S("number") = N("$text",4);
	single();
@RULES
_rank <-
	No [s]	### (1)
	\. [s]	### (2)
	_xWHITE [s star]	### (3)
    _xNUM [s] ### (4)
	@@
		
_companyMarker <-
    _xWILD [s one matches=(inc ltd company firm)] ### (1)
    \. [s optional] ### (2)
    @@