/* ******************************************************************************
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.  All rights reserved.
 ******************************************************************************/

IMPORT $ AS Certification;

EXPORT DataFile := DATASET(Certification.Setup.Filename,
                           {Certification.Layout_FullFormat, UNSIGNED8 __filepos {VIRTUAL(fileposition)}},
                           THOR);
