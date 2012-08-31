/* ******************************************************************************
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.  All rights reserved.
 ******************************************************************************/

IMPORT $ AS Certification;

export IndexFile := INDEX(Certification.DataFile, 
                          {lname,
                           fname,
                           prange,
                           street,
                           zips,
                           age,
                           birth_state,
                           birth_month,
                           __filepos 
                          }, 
                          Certification.Setup.KeyName);
