IMPORT Python;

/*
 This example illustrates and tests the use of embedded Python
*/

// Mapping of exceptions from Python to ECL

integer testThrow(integer val) := EMBED(Python)
raise Exception('Error from Python')
ENDEMBED;

// Can't catch an expression(only a dataset)
d := dataset([{ 1, '' }], { integer a, string m} ) : stored('nofold');

d t := transform
  self.a := FAILCODE;
  self.m := FAILMESSAGE;
  self := [];
end;

catch(d(testThrow(a) = a), onfail(t));
