/* ******************************************************************************
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
 ******************************************************************************/

IMPORT $ AS Tutorial;
EXPORT File_OriginalPerson := 
  DATASET('~tutorial::YN::OriginalPerson', Tutorial.Layout_People, THOR);
