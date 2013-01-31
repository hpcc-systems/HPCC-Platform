import python;

string anagram(string word) := EMBED(Python)
  def anagram(w):
    if word == 'cat':
      return 'act'
    else:
      return w

  return anagram(word)
ENDEMBED;

anagram('dog');
anagram('cat');



