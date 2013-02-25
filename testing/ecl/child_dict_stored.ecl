// Test fetching child dictionaries and datasets from stored

// noroxie
// nothor

d1 := dataset(
  [
   { 'a',
    [ { 'b', 'c' }],
    [ {'d' => 'e' }, {'f' => 'g'} ]
   }
  ],
   {
     string a,
     dataset({string1 c1, string1 c2}) childds,
     dictionary({string1 c1=>string1 c2}) childdict
   }) : stored('d1');

d2 := dictionary(
  [
   { 'a' =>
    [ { 'b', 'c' }],
    [ {'d' => 'e' }, {'f' => 'g'} ]
   }
  ],
   {
     string a =>
     dataset({string1 c1, string1 c2}) childds,
     dictionary({string1 c1=>string1 c2}) childdict
   }) : stored('d2');

output(d1);
output(d2);
