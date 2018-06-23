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
REAL8 fmod(REAL8 numer, REAL8 denom, UNSIGNED1 dbz) : eclrtl,pure,include,library='eclrtl',entrypoint='rtlFMod';
BOOLEAN fmatch(REAL8 a, REAL8 b, REAL8 epsilon = 0.0) : eclrtl,pure,include,library='eclrtl',entrypoint='rtlFMatch';
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

/**
 * Returns the floating-point remainder of numer/denom (rounded towards zero).
 * If denom is zero, the result depends on the -fdivideByZero flag:
 *   'zero' or unset: return zero.
 *   'nan': return a non-signalling NaN value
 *   'fail': throw an exception
 * 
 * @param numer           The numerator.
 * @param denom           The denominator.
 */

EXPORT REAL8 FMod(REAL8 numer, REAL8 denom) := 
  rtl.FMod(numer, denom,
    CASE(__DEBUG__('divideByZero'),
      'nan'=>2,
      'fail'=>3,
      1));

/**
 * Returns whether two floating point values are the same, within margin of error epsilon.
 * 
 * @param a           The first value.
 * @param b           The second value.
 * @param epsilon     The allowable margin of error.
 */

EXPORT BOOLEAN FMatch(REAL8 a, REAL8 b, REAL8 epsilon=0.0) := rtl.FMatch(a, b, epsilon);

END;
