import Javascript;

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
