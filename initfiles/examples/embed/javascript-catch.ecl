IMPORT javascript;

/*
 This example illustrates and tests the use of embedded JavaScript
*/

// Mapping of exceptions from JavaScript to ECL

integer testThrow(integer val) := EMBED(javascript) throw new Error("Error from JavaScript"); ENDEMBED;

d := dataset([{ 1, '' }], { integer a, string m} ) : stored('nofold');

d t := transform
  self.a := FAILCODE;
  self.m := FAILMESSAGE;
  self := [];
end;

catch(d(testThrow(a) = a), onfail(t));
