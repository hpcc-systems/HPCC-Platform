/* ******************************************************************************
## Copyright © 2011 HPCC Systems.  All rights reserved.
 ***************************************************************************** */

// Clean up the actor name in the records

// This function is actually only used in one place and therefore it would really be 
// more logical to leave it in the ECL file from which it is used. 
// However, we have split it out here to give an idea of how a library of support functions 
// could be built up to encourage modularity and code-reuse. For instance more sophisticated
// clean up rules may need to be applied later on.
  
IMPORT Std;

EXPORT STRING CleanActor(STRING infld) := FUNCTION
  //this can be refined later
  s1 := Std.Str.FindReplace(infld, '\'',''); // replace apostrophe
  s2 := Std.Str.FindReplace(s1, '\t','');    //replace tabs
  s3 := Std.Str.FindReplace(s2, '----','');  // replace multiple -----
  return TRIM(s3, LEFT, RIGHT);
END;
