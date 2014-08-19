import Javascript;
/*
 This example illustrates and tests the use of embedded JavaScript.
 In this example the javascript that is embedded is more complex, including a definition of a function
 */

string anagram(string word) := EMBED(Javascript)

function anagram(word)
{
  if (word == 'cat')
    return 'act';
  else
    return word;
}

anagram(word)
ENDEMBED;

anagram('dog');
anagram('cat');