###############################################
# FILE: vocabulary.pat
# SUBJ: comment here
# AUTH: David de Hilster
# CREATED: 14/Nov/00 11:45:54
# MODIFIED:
###############################################

@PATH _ROOT _paragraph _sentence

@POST
	S("compare") = ">";
	"tester.txt" << "testing!\n";
	single();
@RULES
_compare <-
    more [s] ### (1)
    than [s] ### (2)
    @@
	
@RULES
	
_pro <-
    _xWILD [s one matches=(it he she its all they them those)] ### (1)
    @@

_prep <-
    _xWILD [s one matches=(as at in of with into)] ### (1)
    @@	
	
_be <-
    _xWILD [s one matches=(is be are was were)] ### (1)
    @@
	
_det <-
    _xWILD [s one matches=(a the an some this these)] ### (1)
    @@
			
_conj <-
    _xWILD [s one matches=(and \, or \&)] ### (1)
    @@
	
_have <-
    _xWILD [s one matches=(have has had offer offers offered include includes)] ### (1)
    @@
	
_conditional <-
    _xWILD [s one matches=(could can should)] ### (1)
    @@

_that <-
    _xWILD [s one matches=(that which)] ### (1)
    @@

_to <-
    _xWILD [s one matches=(to)] ### (1)
    @@

_special <-
    _xWILD [s one matches=(only when so whenever whatever)] ### (1)
    @@

_direction <-
    _xWILD [s one matches=(up down steady)] ### (1)
    @@
	
_found <-
    _xWILD [s one matches=(found founds founded founding)] ### (1)
    @@

_defend <-
    _xWILD [s one matches=(defend defends defended defending)] ### (1)
    @@

_adj <-
    _xWILD [s one matches=(last latest)] ### (1)
    @@

_adv <-
    _xWILD [s one matches=(substantially)] ### (1)
    @@

_position <-
    _xWILD [s one matches=(above below)] ### (1)
    @@

_field <-
    _xWILD [s one matches=(area field)] ### (1)
    @@

_moneyType <-
    _xWILD [s one matches=(cash currency)] ### (1)
	@@
	
@POST
	S("action") = "acquire";
	single();
@RULES
_acquire <-
    _xWILD [s one matches=(acquire acquires acquired acquiring)] ### (1)
    @@
	
@POST
	S("action") = "buy";
	single();
@RULES
_buyEvent <-
    _xWILD [s one matches=(purchase sale)] ### (1)
    @@
	
_buy <-
    _xWILD [s one matches=(buy buys bought)] ### (1)
    @@