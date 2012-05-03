var shards = ["rnd-nosql-node1", "rnd-nosql-node2", "rnd-nosql-node3", "rnd-nosql-node4"];

for (var i = 0; i < shards.length; i++) {
    db.runCommand({
        addshard : shards[i]
    });
}
db.runCommand({
    enablesharding : "UserDatabase"
});
db.runCommand({
    shardcollection : "UserDatabase.UserTable", key : { _id : 1 }
});