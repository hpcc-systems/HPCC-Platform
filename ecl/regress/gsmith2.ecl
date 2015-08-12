/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

string2048 in_content := '' : stored('Content');
unsigned in_category := 0 : stored('Category');
unsigned in_format := 0 : stored('Format');

result_score :=
    record
        integer score;
    end;

result_address :=
    record(result_score)
        string street :='';
        string city := '';
        string2 st := '';
        string5 zip5 := '';
  end;

result_person :=
    record(result_score)
        string fname :='';
        string lname := '';
        string12 did := '';
        dataset(result_address) addresses;
  end;

result_company :=
    record(result_score)
        string name :='';
        dataset(result_address) addresses;
        dataset(result_person) people;
  end;

entity :=
  record
      string orig := '';
        integer pos := 0;
    end;

entity_address :=
    record(entity)
        dataset(result_address) addresses;
  end;

entity_person :=
    record(entity)
        dataset(result_person) people;
  end;

entity_company :=
    record(entity)
        dataset(result_company) companies;
  end;

dataset_entity_address := dataset(
        [   {'Turn right off Clint Moore before the I95', 158,  [   {100, '6601 Park of Commerce Blvd', 'Boca Raton', 'FL', '33487'},
                                                                                                                        {40, '6602 Park of Commerce Blvd', 'Boca Raton', 'FL', '33487'}
                                                                                                                    ]
            }
        ], entity_address);

output(dataset_entity_address, named('vertices'));

dataset_entity_person := dataset(
        [   {'The bloke who wrote GAB', 999,    [   {99,  'Horatio', 'Smith', '999999999999',    [   {60, '6601 Park of Commerce Blvd', 'Boca Raton', 'FL', '33487'},
                                                                                                                                                                            {99, '4225 Birchwood Drive', 'Boca Raton', 'FL', '33487'}
                                                                                                                                                                        ]
                                                                                    }
                                                                                ,
                                                                                    {-99,   'Scott', 'Snargle', '888888888888',  [   {50, '6601 Park of Commerce Blvd', 'Boca Raton', 'FL', '33487'},
                                                                                                                                                                            {50, 'House with big garage', 'Very West', 'FL', '33487'}
                                                                                                                                                                        ]
                                                                                    }
                                                                                ]
            }
        ], entity_person);

dataset_entity_company := dataset(
        [   {'Some random description', 5,   [   {100, 'Redacted inc',    [   {100, '6601 Park of Commerce Blvd', 'Boca Raton', 'FL', '33487'},
                                                                                    {40, '6602 Park of Commerce Blvd', 'Boca Raton', 'FL', '33487'}
                                                                                ],
                                                        [   {99,  'Horatio', 'Smith', '999999999999',
                                                                                [   {60, '6601 Park of Commerce Blvd', 'Boca Raton', 'FL', '33487'},
                                                                                    {99, '4225 Birchwood Drive', 'Boca Raton', 'FL', '33487'}
                                                                                ]
                                                            }
                                                        ,
                                                            {-99,   'Scott', 'Snargle', '888888888888',
                                                                                [   {50, '6601 Park of Commerce Blvd', 'Boca Raton', 'FL', '33487'},
                                                                                    {50, 'House with big garage', 'Very West', 'FL', '33487'}
                                                                                ]
                                                            }
                                                        ]
                                                                                                    }
                                                                                                ],
            }
        ], entity_company);
output(dataset_entity_address, named('address'));
//output(dataset_entity_address, named('person'));
output(dataset_entity_company, named('company'));
