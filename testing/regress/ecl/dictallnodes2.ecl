/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

//nohthor
//nothor

resistorCodes := dataset([{0, 'Black'},
                          {1, 'Brown'},
                          {2, 'Red'},
                          {3, 'Orange'},
                          {4, 'Yellow'},
                          {5, 'Green'},
                          {6, 'Blue'},
                          {7, 'Violet'},
                          {8, 'Grey'},
                          {9, 'White'}], {unsigned1 value, string color}) : stored('colorMap');

color2code := DICTIONARY(resistorCodes, { color => value});

colourDictionary := dictionary(recordof(color2code));

bands := DATASET([{'Red'},{'Yellow'},{'Blue'}], {string band}) : STORED('bands');

valrec := RECORD
            unsigned1 value;
          END;

valrec getValue(bands L, colourDictionary mapping) := TRANSFORM
  SELF.value := mapping[L.band].value;
END;

results := allnodes(PROJECT(bands, getValue(LEFT, THISNODE(color2code))));

ave(results, value);  // Should remain the same regardless of how many slaves there are
