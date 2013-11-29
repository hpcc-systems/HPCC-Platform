IMPORT STD;

str1 := 'Repeat this string ';

ASSERT(STD.Str.Repeat(str1, 0) = '');
ASSERT(STD.Str.Repeat(str1, 1) = str1);
ASSERT(STD.Str.FindCount(STD.Str.Repeat(str1, 2), str1) = 2);
str4 := STD.Str.Repeat(str1, 10);
ASSERT((STD.Str.FindCount(str4, str1) = 10) & (length(str4) = length(str1)*10) );

str2 := '';

ASSERT(STD.Str.Repeat(str2, 0) = '');
ASSERT(STD.Str.Repeat(str2, 1) = '');
ASSERT(STD.Str.Repeat(str2, 2) = '');
ASSERT(STD.Str.Repeat(str2, 10) = '');

str3 := 'r';

ASSERT(STD.Str.Repeat(str3, 0) = '');
ASSERT(STD.Str.Repeat(str3, 1) = 'r');
ASSERT(STD.Str.Repeat(str3, 2) = 'rr');
ASSERT(STD.Str.Repeat(str3, 10) = 'rrrrrrrrrr');
