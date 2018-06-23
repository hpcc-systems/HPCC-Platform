#option ('allowVariableRoxieFilenames', 1);
import $.setup;
prefix := setup.Files(false, false).FilePrefix;

string suffix := '' : stored('suffix');


d1 := dataset([{1}], { INTEGER a });
d2 := dataset([{2}], { INTEGER a });
d3 := dataset([{3}], { INTEGER a });
i := dataset(prefix + 'outfile' + suffix, { INTEGER a }, FLAT);

sequential(
  output(d1,,prefix + 'outfile'+suffix, OVERWRITE),
  output(i);
  output(d2,,prefix + 'outfile'+suffix, OVERWRITE),
  output(i);
  output(d3,,prefix + 'outfile'+suffix, OVERWRITE),
  output(i);
);
