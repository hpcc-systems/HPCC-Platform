export share := module

export t_IntegerArrayItem := record
    integer value { xpath('')};
end;

export t_StringArrayItem := record
    string value { xpath(''), MAXLENGTH(8192) };
end;

end;
