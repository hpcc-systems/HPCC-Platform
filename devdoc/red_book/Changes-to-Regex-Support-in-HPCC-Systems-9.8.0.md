# Changes to Regex Support in HPCC Systems 9.8.0

**High Priority**

> NOTE: We are publishing this Red Book page before the 9.8.0 release so you can plan any work that may be needed for the Regex changes.

Beginning with version 9.8, HPCC Systems will use a new third-party library to support regular expression operations in the platform. **PCRE2** will replace both **Boost::regex** and **ICU** in regular expression pattern parsing, compilation, and substring replacement. This change brings more consistency to how regex works within ECL, better support for UTF-8 data and overall better performance.

## Affected Functions/Patterns

The ECL functions `REGEXFIND`, `REGEXFINDSET`, and `REGEXREPLACE`. Our regression tests have turned up only **four usage patterns** that need attention. You can make the changes outlined in this document now, in your current (pre-9.8) version of the platform, and be far less surprised when your cluster is eventually upgraded.

> It must be stressed that if your code uses one of the usage patterns outlined in this document then you must update your code.  Failure to do so will result in compilation errors or failing workunits at runtime.

## Questions? Concerns?

Contact Dan S. Camper [dan.camper@lexisnexisrisk.com](mailto:dan.camper@lexisnexisrisk.com)

---

## Pattern #1: Invalid Escape Sequences

When creating a search pattern, you can use a syntax like \d – entered as '\\d' in ECL – to mean "match any numeric digit."  The \d is known as a generic character type and there is an explicit list of them.  Each one of them is defined as a backslash followed by a single ASCII letter.  Regex also has metacharacters, like brackets ('[' and ']'), which define character sets.  If you want to search for a metacharacter in a string, you have to escape it, like \\[.  The combined set of generic character types and escaped metacharacters is the set of "valid escape sequences."

So, what happens if the regex library encounters an escape sequence that is not valid (meaning, the character is not a known character type or metacharacter)?

Both Boost::regex and ICU will treat that character as if it were the literal character instead, but PCRE2 will generate an error.

This affects the search pattern argument (the first argument) within REGEXFIND, REGEXFINDSET, and REGEXREPLACE.

Example:

```ecl
REGEXFIND('\\bBAR\\b|\\bINTO\\b|\\bBAZ\\b', ' MY PINTO HORSE '); //This fails
```

That example is derived from in-house ECL code, and it contains a (probable) logic error.  Each word in the search pattern is wrapped in a \b escape sequence (which means "word boundary") except for INTO; it looks like a copy-and-paste error.

Prior to version 9.8, ECL would accept that REGEXFIND.  The regex libraries would convert that '\\INTO\\b' to 'INTO\\b' and happily search for it.  In this example, the REGEXFIND would succeed because "PINTO " matches.  However, if the developer was really trying to find whole words, matching PINTO would be incorrect.

In version 9.8, PCRE2 looks at '\\I' and sees an invalid escape sequence because that capital-I is not a character type or metacharacter.  A descriptive error message at compile time (if the pattern is a literal string) or at runtime otherwise.

This pattern is the best out of the four because invalid escape sequences are almost always going to be a mistake and fixing them improves our code.

Solution: **Ensure that escape sequences used in your search patterns are correct.**

Example:

```ecl
REGEXFIND('\\bBAR\\b|\\bINTO\\b|\\bBAZ\\b', ' MY PINTO HORSE '); //Working Example
```

## Pattern #2: Long Unicode Property Names

Unicode property names are used to identify entire classes of characters like "uppercase letter" or "numeric digit," specifically within Unicode strings.  There are many property names, and all of them have both long and short versions of those names.  In a search pattern, the syntax for a property name is \p{N} (where N is replaced by the name).

PCRE2 supports only the short Unicode property names.

This affects the search pattern argument (the first argument) within REGEXFIND, REGEXFINDSET, and REGEXREPLACE.  Only Unicode and UTF-8 searches are affected.

Example:

```ecl
REGEXFIND(u'\\p{Letter}', u' MY PINTO HORSE '); //This fails
```

Prior to version 9.8, this code matches the Unicode 'M' in the target string, because that is the first Unicode letter.

In version 9.8, PCRE2 rejects this search pattern with a descriptive error message at compile time (if the pattern is a literal string) or at runtime otherwise.

Solution: **Use only the short version of a Unicode property name in your search patterns.**

Example:

```ecl
REGEXFIND(u'\\p{L} ', u' MY PINTO HORSE '); //Working example
```

## Pattern #3: POSIX Syntax for Short Unicode Property Names

The ICU library used prior to  HPCC Systems 9.8 supports a unique syntax that some developers are aware of:  short Unicode property names with POSIX-style character classes.

As far as character classes go, POSIX concerns itself only with plain ASCII strings.  POSIX defines a few well-known character classes like [[:upper:]], which means "uppercase character."

ICU supports combining that simple syntax with short Unicode property names, so you can use something like [[:Lu:]] instead of the standard [\p{Lu}] in your pattern.  This is an ICU-specific syntax that is not supported by PCRE2.  PCRE2 will reject it with a descriptive error.

This affects the search pattern argument (the first argument) within REGEXFIND, REGEXFINDSET, and REGEXREPLACE.  Only Unicode and UTF-8 searches are affected.

Example:

```ecl
REGEXFIND(u'[[:Lu:]]', u' MY PINTO HORSE ');  //This fails
```

Prior to version 9.8, that code would match the 'M' character in the target, as it is the first uppercase Unicode letter.

In version 9.8, PCRE2 rejects this search pattern with a descriptive error message at compile time (if the pattern is a literal string) or at runtime otherwise.

Solution: **Use the [\p{N} ] syntax instead of [[:N:]] syntax in your Unicode search arguments.**

Example:

```ecl
REGEXFIND(u'[\\p{Lu}]', u' MY PINTO HORSE '); //Working example
```

## Pattern #4: Backslashes and Numbered Capture Groups

A regex capture group is a feature in regular expressions that allows you to extract a specific part of the matched text. By placing a portion of the regex pattern inside parentheses, you create a capture group. The text that matches this part of the pattern can then be referenced later, either within the same regular expression or during replacement. Capture groups are numerically indexed, starting from 1 for the first group, 2 for the second, and so on. The entire match (the whole regex pattern) is referred to as group 0.

There are two different syntaxes to refer to a numbered capture group: Either with a backslash or with a dollar sign.  Referring to the first capture group, for example, is either '\1' or '$1'.

There are two different places you can use capture groups: Within the search pattern, to refer to something that matched earlier, and in the replacement text. Perl deprecated using the backslash syntax in replacement strings quite some time ago.

**Within a search pattern, you should always use the backslash syntax.**  This rule is unchanged for our regex libraries, even PCRE2.  **However, within the replacement text, PCRE2 supports only the dollar sign syntax.**

This affects the replacement string argument (the third argument) within REGEXREPLACE.

Example:

```ecl
REGEXREPLACE('(\\d{3})-\\1-(\\d{4})', '512-512-5555', 'XXX-XXX-\\2'); //This fails
```

 Prior to version 9.8, this code would produce 'XXX-XXX-5555' as a result.

 In version 9.8, PCRE2 rejects this syntax with a "bad escape sequence" error (similar to Pattern #1, above).

 Solution: **Use the $ syntax to refer to capture groups in replacement strings rather than the backslash syntax.  The backslash syntax is required for use in search patterns.**

Example:

```ecl
REGEXREPLACE('(\\d{3})-\\1-(\\d{4})', '512-512-5555', 'XXX-XXX-$2'); //Working example
```

## Pattern #5: POSIX class names

The old Regex libraries allowed illegal "bare" POSIX character class names but PCRE2 does not. You should modify your code where needed as shown in the following example:

```ecl
//If you have this:
REGEXFIND('[:alpha:]', 'Whatever'); 
//You should now use this:
REGEXFIND('[[:alpha:]]', 'Whatever');
```

## Pattern #6: Dot character (.) does not match newline characters by default

The dot character (.) **does not**  match newline characters by default.

The old behavior (pre-9.10) was different depending on data type (STRING vs UNICODE). With STRINGs, dot **does** match newline; with UNICODE, dot **does not** match. This is a great example of why we changed to a single regex engine. If you need to match "any character including newlines" change the pattern:

```ecl
//If you have this:
REGEXFIND('.+' , 'Foo.\\nBar');
//You should now use this:
REGEXFIND('(?:.|\n)+', 'Foo.\\nBar');
```
