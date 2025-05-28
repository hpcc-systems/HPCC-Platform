#option ('allowVariableRoxieFilenames', 1);

import Sleep from Std.System.Debug;
import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;

string suffix := '' : stored('suffix');


d1 := dataset([{1}], { INTEGER a });
d2 := dataset([{2}], { INTEGER a });
d3 := dataset([{3}], { INTEGER a });
i := dataset(prefix + 'outfile' + suffix, { INTEGER a }, FLAT);

sequential(
  output(d1,,prefix + 'outfile'+suffix, OVERWRITE),
  output(i);
  // Roxie caches files by crc and modification date
  // Only 1 second granularity can be guaranteed on the modification dates - so sleep to ensure the update is spotted
  Sleep(1000);
  output(d2,,prefix + 'outfile'+suffix, OVERWRITE),
  output(i);
  // Roxie caches files by crc and modification date
  // Only 1 second granularity can be guaranteed on the modification dates - so sleep to ensure the update is spotted
  Sleep(1000);
  output(d3,,prefix + 'outfile'+suffix, OVERWRITE),
  output(i);
);
