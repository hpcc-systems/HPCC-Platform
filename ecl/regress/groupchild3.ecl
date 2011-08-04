//nothor
//Test iteration of grouped datasets in child queries.

#option ('groupedChildIterators', true);

childRecord := RECORD
string      name;
unsigned    dups := 0;
end;


namesRecord :=
            RECORD
string20        surname;
dataset(childRecord) children;
            END;


ds := dataset(
[
    {'Gavin',
        [{'Smith'},{'Jones'},{'Jones'},{'Doe'},{'Smith'}]
    },
    {'John',
        [{'Bib'},{'Bob'}]
    }
], namesRecord);

namesRecord t(namesRecord l) := transform
    deduped := dedup(group(l.children, name), name);
    cnt(string search) := count(table(deduped(name != search), {count(group)}));
    self.children := group(project(deduped, transform(childRecord, self.dups := cnt(left.name); self.name := left.name)));
    self := l;
end;


output(project(nofold(ds), t(left)));



