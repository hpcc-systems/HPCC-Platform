/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#option ('newQueries', true);

import ghoogle;
import lib_stringLib as *;

ghoogle.ghoogleDefine()

///////////////////////////////////////////////////////////////////////////////////////////////////////////
// An attempt at a operator precedence parse for LN queries - incomplete (and ugly).

parseTextQuery(string query) := function

tokKind := enum(None,
                Word, Wildcarded, Suffixed,
                CloseBra,
                OpenBra, Atleast, Caps, NoCaps, AllCaps, Segment,
                TNot,
                Proximity, TAnd,
                TOr,

                //Pseudo ops used later.
                Terminal);

pattern patWord := pattern('[A-Za-z]+');
pattern patNumber := pattern('[0-9]+');
pattern patQualifier := patWord '(';
pattern openBra := '(';
pattern closeBra := ')';
pattern patAnd := 'AND';
pattern patOr := 'OR';
pattern patNot := 'NOT';
pattern patConnector := opt('PRE') '/' opt(patNumber);
pattern patWildcarded := pattern('[A-Z*a-z]+');
pattern patSuffixed := pattern('[A-Za-z]+!');

pattern ws := [' ','\t'];

pattern matchPattern // order is significant!
    :=  patQualifier
    |   patAnd
    |   patOr
    |   patNot
    |   patConnector
    |   patWord
    |   patWildcarded
    |   patSuffixed
    |   openBra
    |   closeBra
    ;

pattern S := ws* matchPattern;

lexerRecord := record
tokKind         kind;
string          text;
            end;

stackRecord := record(lexerRecord)
termType        term;
stageType       stage;
boolean         invert;
            END;

inFile := dataset([ query ], { string line});
lexerRecord createMatchRecord(inFile l) := transform
        text := matchtext(matchPattern);
        lowerText := StringLib.StringToLowerCase(text);
        self.text := text;
        self.kind := map(
                matched(patQualifier)=>
                    MAP(lowerText[1..7]='atleast'=>tokKind.Atleast,
                        lowerText='caps('=>tokKind.Caps,
                        lowerText='nocaps('=>tokKind.NoCaps,
                        lowerText='allcaps('=>tokKind.AllCaps,
                        tokKind.Segment),
                matched(patConnector)=>tokKind.Proximity,
                matched(patWord)=>tokKind.Word,
                matched(patWildcarded)=>tokKind.Wildcarded,
                matched(patSuffixed)=>tokKind.Suffixed,
                matched(patAnd)=>tokKind.TAnd,
                matched(patOr)=>tokKind.TOr,
                matched(patNot)=>tokKind.TNot,
                matched(openBra)=>tokKind.OpenBra,
                matched(closeBra)=>tokKind.closeBra,
                FAIL(tokKind, 'Unexpected match'));
    END;

tokenStream := parse(inFile, line, S, createMatchRecord(left), first, scan);

ModifierRecord := record
wordFlags       wordFlagMask;
wordFlags       wordFlagCompare;
sourceType      source;
segmentType     segment;
    end;

processRecord := record
dataset(searchRecord) commands;
dataset(searchRecord) terms;
dataset(lexerRecord) input;
dataset(stackRecord) stack;
ModifierRecord  modifiers;
dataset(ModifierRecord) savedModifiers;
termType numTerms;
    END;


createTerm(ModifierRecord modifiers, lexerRecord term, termType termNum) :=
    dataset(
        row(transform(searchRecord,
        SELF.word := term.text;
        SELF.action := CASE(term.kind, tokKind.word=>actionEnum.ReadWord, actionEnum.None);
        SELF := modifiers;
        SELF.term := termNum;
        SELF := [];
        )));


processRecord processAddTerminal(processRecord in) :=
    PROJECT(in,
        TRANSFORM(processRecord,
            SELF.terms := in.terms + createTerm(in.modifiers, in.input[1], in.numTerms+1);
            SELF.input := in.input[2..];
            SELF.numTerms := in.numTerms + 1;
            SELF := in));

processRecord processShift(processRecord in) := function
    nextSymbol := in.input[NOBOUNDCHECK 1];
    return PROJECT(in,
        TRANSFORM(processRecord,
            SELF.stack := in.stack + row(transform(stackRecord, self := nextSymbol; self := []));
            SELF.input := in.input[2..];
            SELF.savedModifiers := left.savedModifiers + left.modifiers;
            SELF.modifiers := CASE(nextSymbol.kind,
                tokKind.Segment=>row(transform(ModifierRecord,
                        self.segment := 0;  // more: need to look up.
                        self := in.modifiers)),
                tokKind.Caps=>row(transform(ModifierRecord,
                        self.wordFlagMask := in.modifiers.wordFlagMask | WordFlags.HasUpper;
                        self.wordFlagCompare := in.modifiers.wordFlagCompare | WordFlags.hasUpper;
                        self := in.modifiers)),
                tokKind.NoCaps=>row(transform(ModifierRecord,
                        self.wordFlagMask := in.modifiers.wordFlagMask | WordFlags.HasUpper;
                        self.wordFlagCompare := in.modifiers.wordFlagCompare & ((wordFlags)(-1) - WordFlags.hasUpper);
                        self := in.modifiers)),
                tokKind.AllCaps=>row(transform(ModifierRecord,
                        self.wordFlagMask := in.modifiers.wordFlagMask | (WordFlags.HasUpper|WordFlags.HasLower);
                        self.wordFlagCompare := (in.modifiers.wordFlagCompare & ((wordFlags)(-1) - WordFlags.hasUpper) | wordFlags.hasUpper);
                        self := in.modifiers)),
                in.modifiers);
            SELF := in));
end;

processRecord processReduce(processRecord in) := function

    numTerms := count(in.terms);
    stageType nextStage := count(in.commands)+1;
    stackRecord reduceOp := in.stack[count(in.stack)];
    numTermsReduced := MAP(reduceOp.kind in [tokKind.Proximity, tokKind.TAnd, tokKind.TOr] => 2, 1);

    searchRecord createCommand() :=
        case(reduceOp.kind,
            tokKind.TAnd=>row(CmdTermAndTerm(in.terms[numTerms-1].term, in.terms[numTerms-2].term)),
            row(transform(searchRecord, SELF := [])));


    return project(in,
        transform(processRecord,
            SELF.commands := in.commands + createCommand();
            SELF.stack := in.stack[1..count(in.stack)-1];
            SELF.terms := in.terms[1..count(in.terms)-numTermsReduced] + createCommand();
            self := in)
        );
end;


processRecord processCloseBra(processRecord l) := function
    topSymbol := l.stack[count(l.stack)];
    return MAP(
        topSymbol.kind in [tokKind.Segment, tokKind.Caps, tokKind.NoCaps, tokKind.allCaps, tokKind.OpenBra]=>
            project(l, TRANSFORM(processRecord,
                SELF.input := l.input[2..];
                SELF.stack := l.stack[1..count(l.stack)-1];
                SELF := l)),
        topSymbol.kind in [tokKind.Atleast]=>
            project(l, TRANSFORM(processRecord,
                SELF.commands := l.commands; // + the atleast command
                SELF.input := l.input[2..];
                SELF.stack := l.stack[1..count(l.stack)-1];
                //MORE: Change terms as well - replace with new command
                SELF := l)),
        processReduce(l));
end;



boolean doShift(tokKind l, tokKind r) :=
    map (
        r in [tokKind.OpenBra, tokKind.Atleast, tokKind.Caps, tokKind.NoCaps, tokKind.AllCaps, tokKind.Segment]=>true,
        r in [tokKind.TNot]=>true,
        r in [tokKind.Proximity, tokKind.TAnd]=>l not in [tokKind.TNot],
        r in [tokKind.TOr] => false,
        false);

processRecord processNext(processRecord in) := function

    nextSymbol := in.input[1];
    topSymbol := in.stack[count(in.stack)];

    return MAP(
        nextSymbol.kind in [tokKind.Word, tokKind.Wildcarded, tokKind.Suffixed] => processAddTerminal(in),
        nextSymbol.kind in [tokKind.CloseBra] => processCloseBra(in),
        doShift(topSymbol.kind, nextSymbol.kind) => processShift(in),
        processReduce(in)
        );
end;


finishedProcessing(processRecord in) := not exists(in.input) and not exists(in.stack);

input := dataset(row(transform(processRecord, SELF.input := tokenStream; SELF := [])));

sequence := LOOP(input, finishedProcessing(rows(left)[NOBOUNDCHECK 1]), project(rows(left), transform(processRecord, self := processNext(LEFT))));

return normalize(sequence, left.commands, transform(right));

end;

output(parseTextQuery('Gavin and Mia'));

