#option ('allowVariableRoxieFilenames', 1);
string prefix := '' : stored('prefix');


d1 := dataset([{1}], { INTEGER a });
d2 := dataset([{2}], { INTEGER a });
d3 := dataset([{3}], { INTEGER a });
i := dataset('regress::outfile'+prefix, { INTEGER a }, FLAT);

sequential(
  output(d1,,'regress::outfile'+prefix, OVERWRITE),
  output(i);
  output(d2,,'regress::outfile'+prefix, OVERWRITE),
  output(i);
  output(d3,,'regress::outfile'+prefix, OVERWRITE),
  output(i);
);
