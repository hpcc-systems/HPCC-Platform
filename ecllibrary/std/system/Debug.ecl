/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

/* Some internal functions implemented in the run time support library which can be useful */

rtl := SERVICE
 unsigned4 msTick() :       eclrtl,library='eclrtl',entrypoint='rtlTick';
 unsigned4 sleep(unsigned4 _delay) : eclrtl,action,library='eclrtl',entrypoint='rtlSleep';
END;

import lib_parselib;

RETURN MODULE

/*
 * Pauses processing by sleeping for a while.
 * 
 * @param millis        The time in milliseconds to sleep for.
 */

EXPORT Sleep(integer millis) := EVALUATE(rtl.Sleep(millis));

/*
 * Returns a millisecond count of elapsed time.
 * 
 */

EXPORT UNSIGNED4 msTick() := rtl.msTick();

/*
 * Returns a textual representation of the parse tree for a pattern match.  
 * It may be useful for debugging PARSE operations.
 * It uses square brackets (such as: a[b[c]d] ) to indicate nesting.  This attribute is only valid
 * within the RECORD or TRANSFORM structure that defines the result of a PARSE operation.
 */

EXPORT STRING getParseTree() := lib_parselib.ParseLib.getParseTree();

/*
 * Returns an xml representation of the parse tree for a pattern match.  
 * It may be useful for debugging PARSE operations.  This attribute is only valid
 * within the RECORD or TRANSFORM structure that defines the result of a PARSE operation.
 */

EXPORT STRING getXmlParseTree() := lib_parselib.ParseLib.getXmlParseTree();

END;