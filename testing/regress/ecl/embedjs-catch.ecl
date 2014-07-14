//nothor

//Thor doesn't handle CATCH properly, see HPCC-9059
//skip type==thorlcr TBD

IMPORT javascript;

integer testThrow(integer val) := EMBED(javascript) throw new Error("Error from JavaScript"); ENDEMBED;
// Test exception throwing/catching
d := dataset([{ 1, '' }], { integer a, string m} ) : stored('nofold');

d t := transform
  self.a := FAILCODE;
  self.m := FAILMESSAGE;
  self := [];
end;

catch(d(testThrow(a) = a), onfail(t));
