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
        [   {'The bloke who wrote GAB', 999,    [   {99,  'Gordon', 'Smith', '999999999999',    [   {60, '6601 Park of Commerce Blvd', 'Boca Raton', 'FL', '33487'},
                                                                                                                                                                            {99, '4225 Birchwood Drive', 'Boca Raton', 'FL', '33487'}
                                                                                                                                                                        ]
                                                                                    }
                                                                                , 
                                                                                    {-99,   'Scott', 'Wagner', '888888888888',  [   {50, '6601 Park of Commerce Blvd', 'Boca Raton', 'FL', '33487'}, 
                                                                                                                                                                            {50, 'House with big garage', 'Very West', 'FL', '33487'}
                                                                                                                                                                        ]
                                                                                    }
                                                                                ]
            }
        ], entity_person);

dataset_entity_company := dataset(
        [   {'Owner from Boca sold to LexisNexis', 5,   [   {100, 'Seisint',    [   {100, '6601 Park of Commerce Blvd', 'Boca Raton', 'FL', '33487'},
                                                                                    {40, '6602 Park of Commerce Blvd', 'Boca Raton', 'FL', '33487'}
                                                                                ], 
                                                        [   {99,  'Gordon', 'Smith', '999999999999',    
                                                                                [   {60, '6601 Park of Commerce Blvd', 'Boca Raton', 'FL', '33487'},
                                                                                    {99, '4225 Birchwood Drive', 'Boca Raton', 'FL', '33487'}
                                                                                ]
                                                            }
                                                        , 
                                                            {-99,   'Scott', 'Wagner', '888888888888',  
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
