IMPORT Python3 AS Python;
/*
 This example illustrates and tests the use of embedded Python.
 In this example the python that is embedded is more complex, including a definition of a function
 */


STRING anagram(string word) := EMBED(Python)
  def anagram(w):
    if word == 'cat':
      return 'act'
    else:
      return w

  return anagram(word)
ENDEMBED;

anagram('dog');
anagram('cat');