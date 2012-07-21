//var replicaSet = "shard1";
//var members = ["ycsb-node1:27017", "ycsb-node2:27018"];
//var replicaSet = "shard2";
//var members = ["ycsb-node2:27017", "ycsb-node1:27018"];
//var replicaSet = "shard3";
//var members = ["ycsb-node3:27017", "ycsb-node4:27018"];
var replicaSet = "shard4";
var members = ["ycsb-node4:27017", "ycsb-node3:27018"];

var replicaSetConfig = {
    _id : replicaSet,
    members : []
};

for (var i = 0; i < members.length; i++) {
    replicaSetConfig.members.push({_id : i, host : members[i]});
}

rs.initiate(replicaSetConfig);

// By default, clients will direct reads to the primary node in a cluster. To distribute reads to secondary nodes, most
// drivers allow you to configure read preference on a per-connection or per-operation basis. To enable secondary reads:
rs.slaveOk();

rs.status();