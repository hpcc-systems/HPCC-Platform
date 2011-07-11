/* ******************************************************************************
## Copyright © 2011 HPCC Systems.  All rights reserved.
 ******************************************************************************/


IMPORT $ AS Tutorial;
IMPORT Std;

Tutorial.Layout_People toUpperPlease(Tutorial.Layout_People pInput) := TRANSFORM 
  SELF.FirstName := Std.Str.ToUpperCase(pInput.FirstName); 
  SELF.LastName := Std.Str.ToUpperCase(pInput.LastName); 
  SELF.MiddleName := Std.Str.ToUpperCase(pInput.MiddleName); 
  SELF.Zip := pInput.Zip; 
  SELF.Street := pInput.Street; 
  SELF.City := pInput.City; 
  SELF.State := pInput.State; 
END ; 

OrigDataset := Tutorial.File_OriginalPerson;
UpperedDataset := PROJECT(OrigDataset,toUpperPlease(LEFT)); 
OUTPUT(UpperedDataset,,'~tutorial::YN::TutorialPerson', OVERWRITE);