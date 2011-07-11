/* ******************************************************************************
## Copyright © 2011 HPCC Systems.  All rights reserved.
 ******************************************************************************/

EXPORT Layout_FullFormat := RECORD
  STRING10 fname;
  STRING10 lname;
  UNSIGNED1 prange;
  STRING10 street;
  UNSIGNED1 zips;
  UNSIGNED1 age;
  STRING2 birth_state;
  STRING3 birth_month;
  UNSIGNED1 one;
  UNSIGNED8 id;
END;