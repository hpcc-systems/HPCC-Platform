IMPORT Python3 as Python;

/*
 This example illustrates and tests the use of embedded Python
*/

// Mapping of exceptions from Python to ECL

INTEGER testThrow(integer val) := EMBED(Python)
  raise Exception('Error from Python')
ENDEMBED;

// Can't catch an expression(only a dataset)
d := DATASET([{ 1, '' }], { INTEGER a, STRING m} ) : STORED('nofold');

d t := TRANSFORM
  self.a := FAILCODE;
  self.m := FAILMESSAGE;
  self := [];
end;

CATCH(d(testThrow(a) = a), onfail(t));
