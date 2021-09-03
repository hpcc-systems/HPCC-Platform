Layouts := module  export _namesLayout    := {
    string _id,
    unicode familyname,
    dataset({unicode firstname}) firstnames,
    dataset({unicode middlename}) middlenames,
    dataset({unicode suffix}) suffixes,
    dataset({unicode firstname}) partnerfirstnames,
    unsigned8 hashkey_familyname         := 0,
    unsigned8 hashkey_firstname          := 0,
    unsigned8 hashkey_middlename      := 0,
    unsigned8 hashkey_suffix              := 0,
    unsigned8 hashkey_partnername      := 0,
    dataset({unsigned8 _hashkey}) hashkey_hasFirstNames := dataset([],{unsigned8 _hashkey}),
    dataset({unsigned8 _hashkey}) hashkey_hasMiddleNames := dataset([],{unsigned8 _hashkey}),
    dataset({unsigned8 _hashkey}) hashkey_missedMiddleNames := dataset([],{unsigned8 _hashkey})
  };

export _expandedNamesLayout    := {
    _namesLayout,
    unicode firstname,
    unicode middlename,
    unicode suffix,
    unicode partnername
  };end;

__names                    := distribute(dataset(
  [
    {u'urn_entityauthority_personauth-12', u'SMITH', dataset([{u'KENNETH'},{u'KEN'}],{unicode firstname}), dataset([{'Z'}],{unicode middlename}), dataset([],{unicode suffix}), dataset([],{unicode firstname})}
  ],Layouts._namesLayout
),hash32(_id));{unsigned it, unsigned fncnt, Layouts._expandedNamesLayout}


getFirstNames(recordof(Layouts._namesLayout) l, integer _cnt) := transform
  self.firstname          := unicodelib.unicodetouppercase(l.firstnames[_cnt].firstname),
  self.familyname         := unicodelib.unicodetouppercase(l.familyname),
  self.hashkey_familyname := hash64(self.familyname),
  self.hashkey_firstname  := hash64(self.firstname),
  self.it                 := _cnt,
  self.fncnt              := count(l.firstnames),
  self                    := l,
  self                    := []
end;

_myNames     := normalize(__names,
                              max(count(left.firstnames),1),
                              getFirstNames(left,counter),local);

_myNamesG     := normalize(__names,
                              max(count(left.firstnames),1),
                              getFirstNames(left,counter));

sequential(
  output(_myNames,named('_myNames')),
  output(_myNamesG,named('_myNamesG')),
);
