# NLP Plugin

This plugin exposes nlp-engine to ECL.  It is a wrapper around VisualText's nlp-engine:
* [NLP-Engine GitHub](https://github.com/VisualText/nlp-engine)
* [VisualText open source software website](https://visualtext.org)


## Installation and Dependencies

The nlp plugin has a dependency on https://github.com/VisualText/nlp-engine which has been added to the HPCC-Platform repository as a git submodule.  To install:
```c
git submodule update --init --recursive
```

## Analyzer Functions

The NLP plugin functions take the analyzer name and text and returns text. One simple way of returning text to be stored in ECL record structures is by using XML. The first time an analyzer is called, it is initialized and subsequent calls to that analyzer will run with the analyzer already in memory.

It is required that the output in NLP++ be piped to [cbuff();](http://visualtext.org/help/cbuf.htm)

### AnalyzeText

```c
STRING AnalyzeText(CONST VARSTRING analyzerName, CONST VARSTRING textToAnalyze)
```

### Unicode AnalyzeText

```c
STRING UnicodeAnalyzeText(CONST VARSTRING analyzerName, CONST VARUNICODE textToAnalyze)
```

### Calling Example Analyzers

```c
IMPORT nlp from lib_nlp; 

text01 := 'The quick brown fox jumped over the lazy boy.';
parsedtext01 := nlp.AnalyzeText('parse-en-us',text01);
output(parsedtext01);

text02 := 'TAI has bought the American Medical Records Processing for more than $130 million dollars.';
parsedtext02 := nlp.AnalyzeText('corporate',text02);
output(parsedtext02);

text03 := 'Right middle lobe consolidation compatible with acute pneumonitis.';
parsedtext03 := nlp.AnalyzeText('parse-en-us',text03);
output(parsedtext03);

text04 := 'TAI\'s stock is up 4% from $58.33 a share to $60.66.';
parsedtext04 := nlp.AnalyzeText('corporate',text04);
output(parsedtext04);
```

### Unicode Version

There is a unicode version of the NLP plugin function:

```c
IMPORT nlp from lib_nlp; 

text01 := 'Sugar Loaf Mountain is Pão de Açúcar';
parsedtext01 := nlp.UnicodeAnalyzeText('parse-en-us',text01);
output(parsedtext01);
```

### Passing Data

This example passes data from the NLP Plugin to ECL using XML:

```c
import nlp from lib_nlp;
import Visualizer;

text01 := 'The quick brown fox jumped over the lazy boy.';
parsedtext01 := nlp.AnalyzeText('parse-en-us',text01);

output(parsedtext01);

p := DATASET([parsedtext01] ,{string line});
vertice := RECORD
    string id := XMLTEXT('id');
    string label := XMLTEXT('label');
END;

edge := RECORD
    string source := XMLTEXT('source');
    string target := XMLTEXT('target');
END;

vertices := PARSE(p, line, vertice, XML('vertice'));
data_vertices := output(vertices, NAMED('graph_vertice'));

edges := PARSE (p, line, edge, XML('edge'));
data_edges := output(edges, NAMED('graph_edges'));

parsetree := Visualizer.Relational.Network('graph', 'graph_vertices',,,,, 'graph_edges',,,,);
```


