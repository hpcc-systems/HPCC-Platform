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

#include "jliball.hpp"
#include "eclrtl.hpp"
#include "thortparse.ipp"

//---------------------------------------------------------------------------

MultiLexer::MultiLexer(const AsciiDfa & _tokens, const AsciiDfa & _skip, const UnsignedArray & _endTokenChars, unsigned _eofId) : tokens(_tokens), skip(_skip)
{
    eofId = _eofId;
    _clear(isEndToken);
    ForEachItemIn(idx, _endTokenChars)
    {
        unsigned next = _endTokenChars.item(idx);
        if (next < 256)
            isEndToken[next] = true;
    }
}

GrammarSymbol * MultiLexer::createToken(symbol_id id, unsigned len, const byte * start)
{
    const FeatureInfo * feature = NULL;     // features[id];
    return new Terminal(id, feature, len, start);
}


position_t MultiLexer::skipWhitespace(position_t pos)
{
    const AsciiDfaState * states = skip.queryStates();
    unsigned * transitions = skip.queryTransitions();
    unsigned activeState = 0;
    const byte * cur = state.start+pos;
    const byte * end = state.end;
    const byte * best = cur;
    loop
    {
        const AsciiDfaState & curState = states[activeState];

        if (curState.accepts())
            best = cur;
        if (cur == end)
            break;
        byte next = *cur++;
        if ((next < curState.min) || (next > curState.max))
            break;
        activeState = transitions[curState.delta + next];
        if (activeState == NotFound)
            break;
    }
    return (size32_t)(best - state.start);
}

unsigned MultiLexer::next(position_t pos, GrammarSymbolArray & symbols)
{
    const byte * start = state.start + skipWhitespace(pos);
    const byte * end = state.end;

    if (start == end)
    {
        symbols.append(*createToken(eofId, 0, start));
        return 1;
    }

    const byte * cur = start;
    unsigned activeState = 0;

    const AsciiDfaState * states = tokens.queryStates();
    unsigned * transitions = tokens.queryTransitions();
    const byte * best = NULL;
    const AsciiDfaState * bestState = NULL;
    loop
    {
        const AsciiDfaState & curState = states[activeState];

        if (curState.accepts())
        {
            best = cur;
            bestState = &curState;
        }
        if (cur == end)
            break;

        byte next = *cur++;
        if ((activeState != 0) && isEndToken[next])
        {
            if (curState.accepts())
            {
                for (unsigned i=0;;i++)
                {
                    unsigned id = tokens.getAccepts(curState, i);
                    if (id == NotFound)
                        break;
                    symbols.append(*createToken(id, (size32_t)(cur-start), start));
                }
                best = NULL;
            }
        }

        if ((next < curState.min) || (next > curState.max))
            break;
        activeState = transitions[curState.delta + next];
        if (activeState == NotFound)
            break;
    }

    if (best)
    {
        for (unsigned i=0;;i++)
        {
            unsigned id = tokens.getAccepts(*bestState, i);
            if (id == NotFound)
                break;
            symbols.append(*createToken(id, (size32_t)(best-start), start));
        }
    }
    return symbols.ordinality();
}

void MultiLexer::setDocument(size32_t len, const void * _start)     
{ 
    state.start = (const byte *)_start; 
    state.end = state.start + len; 
}

#if 0
ToDo:

* Do some more planning re:
  o Augmented grammars
  o Generating the lexer. Especially what we do about unknown words/multiple possible matches.  [Other implictations if tokens do not necessarily lie on the same boundaries].
  o Representing penalties and probabilities.
  o Translating the regex syntax into parser input.
  o Conditional reductions - where how/do they occur? What arguments do they need?
  o Returning multiple rows from a single match?

* Parameterised patterns - how do they related to augmented grammars[do not], and what is needed to implement them?

* Design in detail the table generator
  o LR or LALR?
  o Pathological grammars e.g., S := | S | ...   -> reread and understand doc.  Can we cope?

* Use cases:
  o MAX and BEST()

* Misc
  Error if ": define()" is applied to a pattern
  MAX,MIN in regex implementation
  Stack problems with regex




#endif
