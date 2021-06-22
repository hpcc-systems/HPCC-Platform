NLP Plugin
================

This plugin exposes nlp-engine to ECL.  It is a wrapper around VisualText's nlp-engine:
* [NLP-Engine GitHub](https://github.com/VisualText/nlp-engine)
* [VisualText open source software website](https://visualtext.org)


Installation and Dependencies
------------------------------

The nlp plugin has a dependency on https://github.com/VisualText/nlp-engine which has been added to the HPCC-Platform repository as a git submodule.  To install:
```c
git submodule update --init --recursive
```

Quick Start
------------

Import the nlp plugin library to analyze a text into its syntactic parse tree which is returned as an XML string:
```c
IMPORT nlp from lib_nlp; 

text01 := 'The quick brown fox jumped over the lazy boy.';
parsedtext01 := nlp.AnalyzeText('taiparse',text01);
output(parsedtext01);

text02 := 'TAI has bought the American Medical Records Processing for more than $130 million dollars.';
parsedtext02 := nlp.AnalyzeText('corporate',text02);
output(parsedtext02);

text03 := 'Right middle lobe consolidation compatible with acute pneumonitis.';
parsedtext03 := nlp.AnalyzeText('taiparse',text03);
output(parsedtext03);

text04 := 'TAI\'s stock is up 4% from $58.33 a share to $60.66.';
parsedtext04 := nlp.AnalyzeText('corporate',text04);
output(parsedtext04);
```

### Analyzer Functions

#### AnalyzeText

```c
STRING AnalyzeText(CONST VARSTRING analyzerName, CONST VARSTRING textToAnalyze)
```

Runs the analyzer on the passed text and returns and XML string from the analyzer. The first time an analyzer is called, it is initialized and subsequent calls to that analyzer will run with the analyzer already in memory.

Returns the text that is from the output in the specified NLP++ analyzer.
