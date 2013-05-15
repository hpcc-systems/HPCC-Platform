/*##############################################################################

    Copyright (C) <2010>  <LexisNexis Risk Data Management Inc.>

    All rights reserved. This program is NOT PRESENTLY free software: you can NOT redistribute it and/or modify
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

//UseStandardFiles
//UseTextSearch
//tidyoutput
//nothor
//nothorlcr
//UseIndexes
//xxvarskip type==roxie && setuptype==thor && !local

#option ('checkAsserts',false)

//SingleQuery := 'AND("the":1, "software":2, "source":3)';
//SingleQuery := 'AND("the", "software", "source")';

q1 := dataset([

#if (#ISDEFINED(SingleQuery))
            SingleQuery
#else
            'AND("black","sheep")',
            'ANDNOT("black","sheep")',
            'MOFN(2,"black","sheep","white")',
            'MOFN(2,2,"black","sheep","white")',

//Word tests
            '("nonexistant")',
            '("one")',
            'CAPS("one")',
            'NOCAPS("one")',
            'ALLCAPS("one")',
            '"ibm"',                                        // simple word, and an alias

//Or tests
            'OR("nonexistant1", "nonexistant2")',           // neither side matches
            'OR("nonexistant1", "sheep")',                  // RHS matches
            'OR("twinkle", "nonexistant2")',                // LHS matches
            'OR("twinkle", "twinkle")',                     // should dedup
            'OR("sheep", "black")',                         // matches in same document
            'OR("sheep", "twinkle")',                       // matches in different documents
            'OR("one", "sheep", "sheep", "black", "fish")', // matches in different documents
            'OR(OR("one", "sheep"), OR("sheep", "black", "fish"))', // matches in different documents

//And tests
            'AND("nonexistant1", "nonexistant2")',          // neither side matches
            'AND("nonexistant1", "sheep")',                 // RHS matches
            'AND("twinkle", "nonexistant2")',               // LHS matches
            'AND("twinkle", "twinkle")',                    // should dedup
            'AND("sheep", "black")',                        // matches in same document
            'AND("sheep", "twinkle")',                      // matches in different documents
            'AND("in", "a")',                               // high frequencies
            'AND("twinkle", "little", "how", "star")',      // Nary
            'AND(AND("twinkle", "little"), AND("how", "star"))',        // Nested
            'AND(NOTAT(AND("twinkle", "little"), 9999), NOTAT(AND("how", "wonder"),8888))',     // Nested
            'AND(AND("twinkle", "little"), AND("how", "wonder"))',      // Nested

//MORE: Should also test segment restriction....

            'ANDNOT("nonexistant1", "nonexistant2")',       // neither side matches
            'ANDNOT("nonexistant1", "sheep")',              // RHS matches
            'ANDNOT("twinkle", "nonexistant2")',            // LHS matches
            'ANDNOT("twinkle", "twinkle")',                 // should dedup
            'ANDNOT("sheep", "black")',                     // matches in same document
            'ANDNOT("sheep", "twinkle")',                   // matches in different documents
            'ANDNOT("one", OR("sheep", "black", "fish"))',      // matches one, but none of the others

//Phrases
            'PHRASE("nonexistant1", "nonexistant2")',       // words don't occour
            'PHRASE("in", "are")',                          // doesn't occur, but words do
            'PHRASE("baa", "black")',                       // occurs, but
            'PHRASE("x", "y", "x", "x", "y")',              // a partial match, first - doesn't actually make it more complicatied to implement
            'PHRASE("james","arthur","stuart")',            // check that next occurence of stuart takes note of the change of document.
            'PHRASE(OR("black","white"),"sheep")',          // proximity on a non-word input
            'PHRASE("one", "for", OR(PHRASE("the","master"),PHRASE("the","dame"),PHRASE("the","little","boy")))',
                                                            // more complex again

//Testing range
            'PHRASE1to5("humpty","dumpty")',
            'PHRASE1to5("together","again")',

//M of N
            'MOFN(2, "humpty", "horses", "together", "beansprout")',    // m<matches
            'MOFN(3, "humpty", "horses", "together", "beansprout")',    // m=matches
            'MOFN(4, "humpty", "horses", "together", "beansprout")',    // m>matches
            'MOFN(2,2, "humpty", "horses", "together", "beansprout")',  // too many matches
            'MOFN(2, "nonexistant", "little", "bo")',                   // first input fails to match any
            'MOFN(2, "little", "bo", "nonexistant")',                   // lose an input while finising candidates
            'MOFN(2, "one", "two", "three", "four", "five")',
            'MOFN(2, "nonexistant", "two", "three", "four", "five")',
            'MOFN(2, "one", "nonexistant", "three", "four", "five")',
            'MOFN(2, "nonexistant1", "nonexistant2", "three", "four", "five")',
            'MOFN(2, "nonexistant1", "nonexistant2", "nonexistant3", "four", "five")',
            'MOFN(2, "nonexistant1", "nonexistant2", "nonexistant3", "nonexistant4", "five")',
            'MOFN(2, PHRASE("little","bo"),PHRASE("three","bags"),"sheep")',    // m of n on phrases
            'MOFN(2, PHRASE("Little","Bo"),PHRASE("three","bags"),"sheep")',    // m of n on phrases - capital letters don't match
            'MOFN(2, OR("little","big"), OR("king", "queen"), OR("star", "sheep", "twinkle"))',

//Proximity
            'PROXIMITY("nonexistant1", "nonexistant2", -1, 1)',
            'PROXIMITY("black", "nonexistant2", -1, 1)',
            'PROXIMITY("nonexistant1", "sheep", -1, 1)',

//Adjacent checks
            'PROXIMITY("ship", "sank", 0, 0)',                      // either order but adjacent
            'NORM(PROXIMITY("ship", "sank", 0, 0))',
            'PROXIMITY("ship", "sank", -1, 0)',                     // must follow
            'PROXIMITY("ship", "sank", 0, -1)',                     // must preceed
            'PROXIMITY("sank", "ship", 0, 0)',                      // either order but adjacent
            'PROXIMITY("sank", "ship", -1, 0)',                     // must follow
            'PROXIMITY("sank", "ship", 0, -1)',                     // must preceed

//Within a distance of 1
            'PROXIMITY("ship", "sank", 1, 1)',                      // either order but only 1 intervening word
            'PROXIMITY("ship", "sank", -1, 1)',                     // must follow
            'PROXIMITY("ship", "sank", 1, -1)',                     // must preceed
            'PROXIMITY("sank", "ship", 1, 1)',                      // either order but only 1 intervening word
            'PROXIMITY("sank", "ship", -1, 1)',                     // must follow
            'PROXIMITY("sank", "ship", 1, -1)',                     // must preceed

            'PROXIMITY("ship", "sank", 0, 2)',                      // asymetric range

//Within a distance of 2
            'PROXIMITY("ship", "ship", 2, 2)',                      // either order but only 2 intervening word, no dups
                                                                    // *** currently fails because of lack of duplication in lowest merger
            'PROXIMITY("zx", "zx", 5, 5)',                          // "zx (za) zx", "zx (za zx zb zc zd) zx" and "zx (zb zc zd zx)"
            'PROXIMITY(PROXIMITY("zx", "zx", 5, 5), "zx", 1, 1)',   // "zx (za) zx (zb zc zd) zx" - obtained two different ways.
            'NORM(PROXIMITY(PROXIMITY("zx", "zx", 5, 5), "zx", 1, 1))', // as above, but normalized
            'PROXIMITY(PROXIMITY("zx", "zx", 5, 5), "zx", 0, 0)',   // "zx (za) zx (zb zc zd) zx" - can obly be obtained from first
                                                                    // you could imagine -ve left and right to mean within - would need -1,0 in stepping, and appropriate hard condition.

            'PROXIMITY("ibm", "business", 2, 2)',                   // alias doesn't allow matching within itself.
            'PROXIMITY("ibm", "business", 3, 3)',                   // alias should match now with other word
            'PROXIMITY("ibm", "ibm", 0, 0)',                        // aliases and non aliases cause fun.

//More combinations of operators
            'AND(OR("twinkle", "black"), OR("sheep", "wonder"))',
            'OR(AND("twinkle", "sheep"), AND("star", "black"))',
            'OR(AND("twinkle", "star"), AND("sheep", "black"))',
            'AND(SET("twinkle", "black"), SET("sheep", "wonder"))',

//Silly queries
            'OR("star","star","star","star","star")',
            'AND("star","star","star","star","star")',
            'MOFN(4,"star","star","star","star","star")',


//Other operators
            'PRE("twinkle", "twinkle")',
            'PRE(PHRASE("twinkle", "twinkle"), PHRASE("little","star"))',
            'PRE(PHRASE("little","star"), PHRASE("twinkle", "twinkle"))',
            'PRE(PROXIMITY("twinkle","twinkle", 3, 3), PROXIMITY("little", "star", 2, 2))',
            'AFT("twinkle", "twinkle")',
            'AFT(PHRASE("little","star"), PHRASE("twinkle", "twinkle"))',
            'AFT(PHRASE("twinkle", "twinkle"), PHRASE("little","star"))',
            'AFT(PROXIMITY("twinkle","twinkle", 3, 3), PROXIMITY("little", "star", 2, 2))',

// Left outer joins for ranking.
            'RANK("sheep", OR("peep", "baa"))',
            'RANK("three", OR("bags", "full"))',
            'RANK("three", OR("one", "bags"))',


//Non standard variants - AND, generating a single record for the match.  Actually for each cross product as it is currently (and logically) implemented
            'ANDJOIN("nonexistant1", "nonexistant2")',          // neither side matches
            'ANDJOIN("nonexistant1", "sheep")',                 // RHS matches
            'ANDJOIN("twinkle", "nonexistant2")',               // LHS matches
            'ANDJOIN("twinkle", "twinkle")',                    // should dedup
            'ANDJOIN("sheep", "black")',                        // matches in same document
            'ANDJOIN("sheep", "twinkle")',                      // matches in different documents
            'ANDJOIN("in", "a")',                               // high frequencies
            'ANDJOIN("twinkle", "little", "how", "star")',      // Nary

            'ANDNOTJOIN("nonexistant1", "nonexistant2")',       // neither side matches
            'ANDNOTJOIN("nonexistant1", "sheep")',              // RHS matches
            'ANDNOTJOIN("twinkle", "nonexistant2")',            // LHS matches
            'ANDNOTJOIN("twinkle", "twinkle")',                 // should dedup
            'ANDNOTJOIN("sheep", "black")',                     // matches in same document
            'ANDNOTJOIN("sheep", "twinkle")',                   // matches in different documents
            'ANDNOTJOIN("one", OR("sheep", "black", "fish"))',  // matches one, but none of the others

            'MOFNJOIN(2, "humpty", "horses", "together", "beansprout")',    // m<matches
            'MOFNJOIN(3, "humpty", "horses", "together", "beansprout")',    // m=matches
            'MOFNJOIN(4, "humpty", "horses", "together", "beansprout")',    // m>matches
            'MOFNJOIN(2,2, "humpty", "horses", "together", "beansprout")',  // too many matches
            'MOFNJOIN(2, "nonexistant", "little", "bo")',                   // first input fails to match any
            'MOFNJOIN(2, "little", "bo", "nonexistant")',                   // lose an input while finising candidates
            'MOFNJOIN(2, "one", "two", "three", "four", "five")',
            'MOFNJOIN(2, "nonexistant", "two", "three", "four", "five")',
            'MOFNJOIN(2, "one", "nonexistant", "three", "four", "five")',
            'MOFNJOIN(2, "nonexistant1", "nonexistant2", "three", "four", "five")',
            'MOFNJOIN(2, "nonexistant1", "nonexistant2", "nonexistant3", "four", "five")',
            'MOFNJOIN(2, "nonexistant1", "nonexistant2", "nonexistant3", "nonexistant4", "five")',
            'MOFNJOIN(2, PHRASE("little","bo"),PHRASE("three","bags"),"sheep")',    // m of n on phrases
            'MOFNJOIN(2, PHRASE("Little","Bo"),PHRASE("three","bags"),"sheep")',    // m of n on phrases - capital letters don't match
            'MOFNJOIN(2, OR("little","big"), OR("king", "queen"), OR("star", "sheep", "twinkle"))',

            'RANKJOIN("SHEEP", "BLACK")',
            'RANKJOIN("sheep", OR("peep", "baa"))',
            'RANKJOIN("three", OR("bags", "full"))',
            'RANKJOIN("three", OR("one", "bags"))',


//ROLLAND - does AND, followed by a rollup by doc.  Should also check that smart stepping still works through the grouped rollup
            'ROLLAND("nonexistant1", "nonexistant2")',          // neither side matches
            'ROLLAND("nonexistant1", "sheep")',                 // RHS matches
            'ROLLAND("twinkle", "nonexistant2")',               // LHS matches
            'ROLLAND("twinkle", "twinkle")',                    // should dedup
            'ROLLAND("sheep", "black")',                        // matches in same document
            'ROLLAND("sheep", "twinkle")',                      // matches in different documents
            'ROLLAND("in", "a")',                               // high frequencies
            'ROLLAND("twinkle", "little", "how", "star")',      // Nary
            'AND(ROLLAND("twinkle", "little"), ROLLAND("how", "star"))',        // Nary

//Same tests as proximity above, but not calling a transform - merging instead
            'PROXMERGE("ship", "sank", 0, 0)',                      // either order but adjacent
            'PROXMERGE("ship", "sank", -1, 0)',                     // must follow
            'PROXMERGE("ship", "sank", 0, -1)',                     // must preceed
            'PROXMERGE("sank", "ship", 0, 0)',                      // either order but adjacent
            'PROXMERGE("sank", "ship", -1, 0)',                     // must follow
            'PROXMERGE("sank", "ship", 0, -1)',                     // must preceed
            'PROXMERGE("ship", "sank", 1, 1)',                      // either order but only 1 intervening word
            'PROXMERGE("ship", "sank", -1, 1)',                     // must follow
            'PROXMERGE("ship", "sank", 1, -1)',                     // must preceed
            'PROXMERGE("sank", "ship", 1, 1)',                      // either order but only 1 intervening word
            'PROXMERGE("sank", "ship", -1, 1)',                     // must follow
            'PROXMERGE("sank", "ship", 1, -1)',                     // must preceed
            'PROXMERGE("ship", "sank", 0, 2)',                      // asymetric range

//SET should be equivalent to OR
            'SET("nonexistant1", "nonexistant2")',          // neither side matches
            'SET("nonexistant1", "sheep")',                 // RHS matches
            'SET("twinkle", "nonexistant2")',               // LHS matches
            'SET("twinkle", "twinkle")',                    // should dedup
            'SET("sheep", "black")',                        // matches in same document
            'SET("sheep", "twinkle")',                      // matches in different documents
            'SET("one", "sheep", "sheep", "black", "fish")',    // matches in different documents
            'OR(SET("one", "sheep"), SET("sheep", "black", "fish"))',   // matches in different documents

//Testing range
            'PHRASE1to5(PHRASE1to5("what","you"),"are")',
            'PHRASE1to5("what", PHRASE1to5("you","are"))',
            'PHRASE1to5(PHRASE1to5("open","source"),"software")',
            'PHRASE1to5("open", PHRASE1to5("source","software"))',

//Atleast
            'ATLEAST(2, "twinkle")',                                // would something like UNIQUEAND("twinkle", "twinkle") be more efficient???
            'ATLEAST(4, "twinkle")',
            'ATLEAST(5, "twinkle")',
            'ATLEAST(5, AND("twinkle","star"))',
            'AND(ATLEAST(4, "twinkle"),"star")',                    // make sure this still smart steps!
            'AND(ATLEAST(5, "twinkle"),"star")',
            'ATLEAST(1, PHRASE("humpty","dumpty"))',
            'ATLEAST(2, PHRASE("humpty","dumpty"))',
            'ATLEAST(3, PHRASE("humpty","dumpty"))',

            '"little"',
            'IN(name, "little")',
            'NOTIN(name, "little")',
            'IN(suitcase, AND("sock", "shirt"))',
            'IN(suitcase, AND("sock", "dress"))',
            'IN(suitcase, AND("shirt", "dress"))',                  //no, different suitcases..
            'IN(suitcase, OR("cat", "dog"))',                       //no - wrong container
            'IN(box, OR("cat", "dog"))',                            //yes
            'IN(box, IN(suitcase, "shirt"))',
            'IN(suitcase, IN(box, "shirt"))',                       // no other elements in the suitcase, so not valid
            'IN(box, AND(IN(suitcase, "shirt"), "car"))',
            'IN(box, AND(IN(suitcase, "shirt"), "lego"))',          // no, lego isn't in the box...
            'IN(box, MOFN(2, "car", "train", "glue"))',             // really nasty - need to modify the m of n to add position equality!
            'IN(box, MOFN(2, "car", "glue", "train"))',             // and check works in all positions.
            'IN(box, MOFN(2, "glue", "car", "train"))',             //   " ditto "
            'IN(box, MOFN(3, "car", "train", "glue"))',
            'NOTIN(box, AND("computer", "lego"))',
            'NOTIN(box, AND("train", "lego"))',
            'IN(suitcase, PROXIMITY("trouser", "sock", 1, 2))',     // ok.
            'IN(suitcase, PROXIMITY("trouser", "train", 1, 2))',    // no, close enough, but not both in the suitcase
            'IN(suitcase, PROXIMITY("trouser", "dress", 6, 6))',    // no, close enough, but not both in the same suitcase
            'PROXIMITY(IN(suitcase, "trouser"), IN(suitcase, "dress"), 6, 6)',  // yes - testing the proximity of the suitcases, not the contents.

            'IN(S, AND("fish", "alive"))',                          // <s> is the sentence container
            'S(AND("fish", "alive"))',                              // pseudonym for within same sentence -
            'S(AND("fish", "finger"))',                             //
            'S(AND("sheep", "wagging"))',
            'P(AND("sheep", "wagging"))',                           // same paragraph...
            'AND(IN(socks, "fox"),IN(socks, "knox"))',
            'AND(IN(box, "fox"),IN(box, "knox"))',
            'AND(IN(box, IN(socks, "fox")),IN(box, "knox"))',
            'AND(IN(socks, IN(box, "fox")),IN(box, "knox"))',           // yes - because no extra elements in the box.
            'S(PHRASE("black", "sheep"))',
            'IN(name, PHRASE("little", "bo", "peep"))',
            'IN(name, PHRASE("little", "bo", "peep", "has"))',

            'IN(range1, IN(range2, "seven"))',                      // only match 5.3
            'SAME(IN(range1, "seven"), IN(range2, "seven"))',       // only match 5.3
            'OVERLAP(IN(range1, "five"), IN(range2, "ten"))',       // overlapping, match 5.4
            'PROXIMITY(IN(range1, "five"), IN(range2, "ten"), 0, 0)',   // adjacent match 5.4, 5.5
            'PROXIMITY(IN(range1, "five"), IN(range2, "ten"), 1, 1)',   // proximity match 5.4, 5.5
            'PROXIMITY(IN(range1, "five"), IN(range2, "ten"), 2, 2)',   // adjacent match 5.4, 5.5, 5.6

            'ATLEAST(2, IN(suitcase, "sock"))',                     // at least two suitcases containing a sock.
            'ATLEAST(3, IN(suitcase, "sock"))',                     // no

            'IN(box, "train")',                                     // should be 4 matches (since inside nested boxes)

            'IN(suitcase, ATLEAST(1, "sock"))',                     // suitcases containing at least one sock.   (should really optimize away)
            'IN(suitcase, ATLEAST(2, "sock"))',                     // at least two suit cases containing a sock.
            'IN(suitcase, ATLEAST(3, "sock"))',
            'IN(suitcase, ATLEAST(3, OR("sock", "dress")))',        //no
            'IN(suitcase, ATLEAST(3, SET("sock", "dress")))',       //no
            'IN(suitcase, ATLEAST(3, OR("sock", "jacket")))',       //yes...
            'IN(suitcase, ATLEAST(3, SET("sock", "jacket")))',      //yes...
            'IN(box, IN(suitcase, ATLEAST(2, "sock")))',            //yes - box, with one match
            'IN(box, IN(suitcase, ATLEAST(3, "sock")))',            //no -
            'IN(box, ATLEAST(2, IN(suitcase, "sock")))',            //yes -
            'IN(box, ATLEAST(3, IN(suitcase, "sock")))',            //no -
            'IN(box, ATLEAST(2, IN(suitcase, ATLEAST(2, "sock"))))',            //no...
            'IN(box, AND(ATLEAST(2, "train"), ATLEAST(2, "sock")))',    // yes
            'IN(box, AND(ATLEAST(3, "train"), ATLEAST(2, "sock")))',    // no
            'IN(suitcase, AND(ATLEAST(2, "sock"), ATLEAST(2, OR("tights", "dress"))))', // no - different suitcases.
            'IN(suitcase, ATLEAST(2, "sock"))', // yes
            'IN(suitcase, ATLEAST(2, OR("tights", "dress")))',  // yes

//The following example fails - not quite sure how to fix it.
//          'IN(suitcase, OR(ATLEAST(2, "sock"), ATLEAST(2, OR("tights", "dress"))))',  // yes
            'IN(suitcase, ATLEAST(4, AND(ATLEAST(2, "sock"), OR("shirt", "trouser"))))',    // yes - nested atleasts...
            'IN(suitcase, ATLEAST(5, AND(ATLEAST(2, "sock"), OR("shirt", "trouser"))))',    // no

            '_ATLEASTIN_(1, IN(suitcase, "sock"), 1)',                      // suitcases containing at least one sock.   (should really optimize away)
            '_ATLEASTIN_(2, IN(suitcase, "sock"), 1)',                      // at least two suit cases containing a sock.
            '_ATLEASTIN_(3, IN(suitcase, "sock"), 1)',

            'S(ANDNOT("fish", "alive"))',                               // pseudonym for within same sentence -
            'S(ANDNOT("fish", "finger"))',                              //

            'AT("the", 2)',                                             // occurences of 'the' at position 2
            'AT("the", 18)',
            'AT("is", 17)',
            'AND(AT("the", 18),AT("is",17))',

            'AND("gch01", "gch02", "gch04")',
            'AND("gch01", "gch02", "gch10")',

            'AND(SET("and","a"), SET("the", "one"), PHRASE("for","the","dame"))',
            'AND(CAPS("sheep"), "spotted")',
            'AND(CAPS("sheep"), NOCAPS("spotted"))',
            'AND(SET(CAPS("sheep","little")), SET(CAPS("Up","go")))',
            'AND(SET(CAPS("sheep","little")), SET(NOCAPS("Up","go")))',
            'AND(OR(CAPS("sheep"),CAPS("Little")), OR(CAPS("Up"),NOCAPS("go")))',

            'ANDNOT(AND("black","sheep"), "family")',
            'ANDNOT(AND("little","and"), "jack")',

            'BUTNOT("little", PHRASE("little", "star"))',
            'BUTNOTJOIN("little", PHRASE("little", "star"))',
            'BUTNOT("black", PHRASE("black", OR("spotted", "sheep")))',
            'BUTNOTJOIN("black", PHRASE("black", OR("spotted", "sheep")))',

            'AND("the", "software", "source")',
            'AND("the":1, "software":2, "source":3)',
            'AND("the":3, "software":2, "source":1)',

//MORE:
// STEPPED flag on merge to give an error if input doesn't support stepping.
// What about the duplicates that can come out of the proximity operators?
// where the next on the rhs is at a compatible position, but in a different document
// What about inverse of proximity x not w/n y
// Can inverse proximity be used for sentance/paragraph.  Can we combine them so short circuited before temporaries created.
//MORE: What other boundary conditions can we think of.

                ' '

#end

            ], queryInputRecord);

p := project(nofold(q1), doBatchExecute(TS_wordIndex, LEFT, 0x00000200));           // 0x200 forces paranoid order checking on
output(p);
