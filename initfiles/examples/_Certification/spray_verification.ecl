/* ******************************************************************************
## Copyright © 2011 HPCC Systems.  All rights reserved.
 ******************************************************************************/

IMPORT $ AS Certification;

out_file := DATASET(Certification.Setup.Sprayfile, Certification.Layout_FullFormat, THOR);

COUNT(out_file);
OUTPUT(out_file);
