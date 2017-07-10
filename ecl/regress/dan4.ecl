IMPORT Std;
IMPORT python;

SaveTimestamp(keyName, theTimestamp) := FUNCTIONMACRO
    #UNIQUENAME(FunCall);
    LOCAL BOOLEAN %FunCall%(Std.Date.Timestamp_t t) := EMBED(python, '' : server('1.0.1.183'), bucket('org_hpcc_guardhat_smarthat_status'))
    RETURN %FunCall%(theTimestamp);
ENDMACRO;

SaveTimestamp('fubar', 1234567890);
