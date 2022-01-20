person := dataset('person', { unsigned8 person_id, string1 per_sex, unsigned per_ssn, string40 per_first_name, data9 per_cid, unsigned8 xpos }, thor);

zero := 0.0 : stored('zero');
output(power(zero, -1));

output(person,
    {
        POWER(person.person_id,0),
        POWER(person.person_id,1),
        POWER(person.person_id,2),
        POWER(person.person_id,3),
        POWER(person.person_id,4),
        POWER(person.person_id,-1),
        POWER(person.person_id,-2),
        POWER(person.person_id,-3),
        POWER(person.person_id,-4),

        POWER(person.person_id,1.1),
        POWER(person.person_id,2.1),
        POWER(person.person_id,3.1),
        POWER(person.person_id,4.1),
        POWER(person.person_id,-1.1),
        POWER(person.person_id,-2.1),
        POWER(person.person_id,-3.1),
        POWER(person.person_id,-4.1),
        0
    },'out.d00');
