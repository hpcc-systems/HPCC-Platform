/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//Information derived from http://en.wikipedia.org/wiki/Template:HarryPotterFamilyTree
//This example file alone licenced under licence http://creativecommons.org/licenses/by-sa/3.0/
//nothor
//nothorlcr

potterRecord := RECORD
    unsigned id;
    unsigned fatherId;
    unsigned motherId;
    unsigned spouseId;
    string name{maxlength(100)};
end;

potterRecord createCharacter(unsigned id, unsigned fatherId,unsigned motherId, unsigned spouseId, string name) := TRANSFORM
    SELF.id := id;
    SELF.fatherId := fatherId;
    SELF.motherId := motherId;
    SELF.spouseId := spouseId;
    SELF.name := name;
END;

characters := dataset([
    createCharacter(01, 00, 00, 00, 'Marvolo Gaunt'),
    createCharacter(02, 01, 00, 00, 'Morfin Gaunt'),
    createCharacter(03, 01, 00, 04, 'Merope Gaunt'),
    createCharacter(04, 00, 00, 04, 'Tom Riddle Sr'),
    createCharacter(05, 04, 03, 00, 'Tom Marvolo Riddle'),
    createCharacter(06, 00, 00, 07, 'Septimus Weasley'),
    createCharacter(07, 00, 00, 06, 'Cedrella Black'),
    createCharacter(08, 00, 00, 09, 'Mr Dursley'),
    createCharacter(09, 00, 00, 08, 'Mrs Dursley'),
    createCharacter(10, 00, 00, 11, 'Mr Evans'),
    createCharacter(11, 00, 00, 10, 'Mrs Evans'),
    createCharacter(12, 00, 00, 13, 'Mr Potter'),
    createCharacter(13, 00, 00, 12, 'Mrs Potter'),
    createCharacter(14, 00, 00, 15, 'Apolline Delacour'),
    createCharacter(15, 00, 00, 14, 'Monsieur Delacour'),
    createCharacter(16, 00, 00, 17, 'Molly Prewett'),
    createCharacter(17, 06, 07, 16, 'Arthur Weasley'),
    createCharacter(18, 08, 00, 00, 'Marjorie Dursley'),
    createCharacter(19, 09, 00, 20, 'Vernon Dursley'),
    createCharacter(20, 10, 11, 19, 'Petunia Evans'),
    createCharacter(21, 10, 11, 22, 'Lily Evans'),
    createCharacter(22, 12, 13, 21, 'James Potter'),
    createCharacter(23, 14, 15, 00, 'Gabrielle Delacour'),
    createCharacter(24, 17, 16, 00, 'Charles Weasley'),
    createCharacter(25, 17, 16, 00, 'Fred Weasley'),
    createCharacter(26, 19, 20, 00, 'Dudley Dursley'),
    createCharacter(27, 14, 15, 28, 'Fleur Delacour'),
    createCharacter(28, 17, 16, 27, 'William Weasley'),
    createCharacter(29, 17, 16, 00, 'Percy Weasley'),
    createCharacter(30, 17, 16, 31, 'George Weasley'),
    createCharacter(31, 00, 00, 30, 'Angelina Johnson'),
    createCharacter(32, 00, 00, 33, 'Hermione Granger'),
    createCharacter(33, 17, 16, 32, 'Ronald Weasley'),
    createCharacter(34, 17, 16, 35, 'Ginevra Weasley'),
    createCharacter(35, 22, 21, 34, 'Harry Potter'),
    createCharacter(36, 28, 27, 00, 'Victoire Weasley'),
    createCharacter(37, 28, 27, 00, 'Dominique Weasley'),
    createCharacter(38, 28, 27, 00, 'Louis Weasley'),
    createCharacter(39, 30, 31, 00, 'Fred Weasley'),
    createCharacter(40, 30, 31, 00, 'Roxanne Weasley'),
    createCharacter(41, 33, 32, 00, 'Rose Weasley'),
    createCharacter(42, 33, 32, 00, 'Hugo Weasley'),
    createCharacter(43, 35, 34, 00, 'James Potter'),
    createCharacter(44, 35, 34, 00, 'Albus Potter'),
    createCharacter(45, 35, 34, 00, 'Lily Potter'),
    createCharacter(46, 29, 00, 00, 'Molly Weasley'),
    createCharacter(47, 29, 00, 00, 'Lucy Weasley')]);
    
    
matched_character_record := RECORD(potterRecord)
    unsigned degree;
END;    


matched_character_record createMatchedCharacter(potterRecord l, unsigned degree) := TRANSFORM
    SELF := l;
    SELF.degree := degree;
END;

string search_name := 'Marvolo Gaunt' : stored('search_name');
unsigned num_degrees := 2 : stored('num_degrees');


original_person := characters(name = search_name);
original_match := project(original_person, createMatchedCharacter(left, 0));

findRelated(dataset(matched_character_record) search, unsigned degree) := FUNCTION
    allRelated := JOIN(characters, search, 
                        LEFT.id IN [RIGHT.fatherId, RIGHT.fatherId] OR
                        RIGHT.id IN [LEFT.fatherId, LEFT.fatherId] OR
                        (LEFT.spouseId <> 0 AND LEFT.spouseId = RIGHT.spouseId) OR
                        (LEFT.fatherId <> 0 AND LEFT.fatherId = RIGHT.fatherId) OR
                        (LEFT.motherId <> 0 AND LEFT.motherId = RIGHT.motherId), TRANSFORM(LEFT), ALL);
    allNew := JOIN(allRelated, search, LEFT.id = RIGHT.id, transform(LEFT), LEFT ONLY);
    combined := search + PROJECT(allNew, createMatchedCharacter(LEFT, degree));

    //Strange test to reproduce problem with child query inside if in parallel graph
    forceAChildQueryZero := count(NOHOIST(sort(NOFOLD(characters), id)(id = 99999)));
    RETURN IF(forceAChildQueryZero=0, combined);
END;

relations := GRAPH(original_match, num_degrees, findRelated(ROWSET(LEFT)[COUNTER-1], COUNTER),parallel);

user_output_record := RECORD
    matched_character_record.degree;
    matched_character_record.name;
END;

user_output := PROJECT(relations, TRANSFORM(user_output_record, SELF := LEFT));
output(sort(user_output, degree, name));
