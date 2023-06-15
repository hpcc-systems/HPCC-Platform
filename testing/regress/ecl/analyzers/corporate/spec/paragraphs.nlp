###############################################
# FILE: paragraphs.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 14/Nov/00 18:22:14
# MODIFIED:
###############################################

@NODES _ROOT

@RULES
_paragraph [unsealed] <-
    _sentence [s plus]	### (1)
    _xWILD [s matches=(_paragraphSeparator _xEND)] ### (2)
    @@