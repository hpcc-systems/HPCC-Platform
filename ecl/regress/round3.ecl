//Make sure both of these don't lose the extra digit.
output((string)(round(9.9D)) + '\n');
output((string)(round(5D, -1)) + '\n');


output((string)(round(nofold(1.1D), 0)) + '\n');
output((string)(round(nofold(1.1D), 1)) + '\n');
output((string)(round(nofold(1.1D), -1)) + '\n');


output((string)(round(1234567, -2)) + '\n');
output((string)(round(nofold(1234567), -2)) + '\n');
