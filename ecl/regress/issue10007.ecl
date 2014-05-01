TweetRec := RECORD
   STRING word;
   INTEGER id;
END;

TweetsData := DATASET([{'Coke',1},{'Pepsi',1},{'India',1},{'BMW',2},{'Ford',3}],TweetRec);

result1 := DEDUP(TweetsData(word='Pepsi'),id,ALL);
result2 := DEDUP(TweetsData(word='Coke'),id,ALL);

finalResult := JOIN(result1,result2,LEFT.id=RIGHT.id);

finalResult;
