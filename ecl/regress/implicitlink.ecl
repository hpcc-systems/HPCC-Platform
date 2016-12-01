IMPORT Python;

nested := RECORD
  integer value;
END;

parent := RECORD
  DATASET(nested) nest;
END;

DATASET(parent) getP() := EMBED(Python)
  return [
          [
           1,2,3,4
          ],
          [
           5,6,7,8
          ]
         ]
ENDEMBED;

pcode(DATASET(parent) p) := EMBED(Python)
  for child in p:
   for c2 in child.nest:
    print c2.value,
ENDEMBED;

ds := getp(); 
//ds := DATASET([{[{1},{3}]}], parent);
//ds;
pcode(ds);


