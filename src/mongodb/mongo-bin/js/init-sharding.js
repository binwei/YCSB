//var shards = ["ycsb-node1", "ycsb-node2", "ycsb-node3", "ycsb-node4"];
var shards = ["shard1/ycsb-node1:27017,ycsb-node2:27018", "shard2/ycsb-node2:27017,ycsb-node1:27018", "shard3/ycsb-node3:27017,ycsb-node4:27018", "shard4/ycsb-node4:27017,ycsb-node3:27018"];

for (var i = 0; i < shards.length; i++) {
    db.runCommand({
        addshard : shards[i]
    });
}

db.printShardingStatus();

db.runCommand({
    enablesharding : "UserDatabase"
});
db.runCommand({
    shardcollection : "UserDatabase.UserTable", key : { _id : 1 }
});
