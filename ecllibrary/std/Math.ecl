/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.  All rights reserved.
############################################################################## */


rtl := 
    SERVICE : fold
REAL8 Infinity() : eclrtl,pure,include,library='eclrtl',entrypoint='rtlCreateRealInf';
REAL8 Nan() : eclrtl,pure,include,library='eclrtl',entrypoint='rtlCreateRealNull';
BOOLEAN IsInfinite(REAL8 value) : eclrtl,pure,include,library='eclrtl',entrypoint='rtlIsInfinite';
BOOLEAN IsNaN(REAL8 value) : eclrtl,pure,include,library='eclrtl',entrypoint='rtlIsNaN';
BOOLEAN IsFinite(REAL8 value) : eclrtl,pure,include,library='eclrtl',entrypoint='rtlIsFinite';
    END;

EXPORT Math := MODULE

/**
 * Return a real "infinity" value.
 * 
 */
 
EXPORT REAL8 Infinity := rtl.Infinity();

/**
 * Return a non-signalling NaN (Not a Number)value.
 * 
 */
 
EXPORT REAL8 NaN := rtl.NaN();

/**
 * Return whether a real value is infinite (positive or negative).
 * 
 * @param val           The value to test.
 */

EXPORT BOOLEAN isInfinite(REAL8 val) := rtl.isInfinite(val);

/**
 * Return whether a real value is a NaN (not a number) value.
 * 
 * @param val           The value to test.
 */

EXPORT BOOLEAN isNaN(REAL8 val) := rtl.isNaN(val);

/**
 * Return whether a real value is a valid value (neither infinite not NaN).
 * 
 * @param val           The value to test.
 */

EXPORT BOOLEAN isFinite(REAL8 val) := rtl.isFinite(val);

END;
