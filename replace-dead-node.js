const Redis = require('ioredis');
var sleep = require('sleep');
var async = require('async');
const { StringDecoder } = require('string_decoder');
var yesno = require('yesno');


var redisClusterConn = {
    "host": "127.0.0.1",
    "port": 7002
};
var redisCluster = new Redis.Cluster([redisClusterConn]);
var newRedisConn = {
    "host": "127.0.0.1",
    "port": 7003
};
var newRedis = new Redis(newRedisConn);

async.waterfall([
    getClusterInfo,
    parseClusterInfo,
    forgetBrokenNode,
    addNewNode,
    assignSlotsToNewNode
], function(err, results) {
    console.log('done');
});

function getClusterInfo(callback) {
    arbitraryCommand(redisCluster, 'cluster', ['nodes'], function (err, value) {
        var clusterInfo = value.toString();
        var nodes = redisCluster.nodes();

        console.log("cluster info:");
        console.log(clusterInfo);
        callback(null, {
            'clusterInfo': clusterInfo,
            'clusterNodes': nodes
        });
    });
}

function parseClusterInfo(results, callback) {
    var lines = results.clusterInfo.split("\n");
    for (var i in lines) {
        var line = lines[i];

        var regex = /([^\s]+).*disconnected\s+([^\s]+)/g;
        var match = regex.exec(line);
        if (match) {
            results['nodeId'] = match[1];
            results['missingSlots'] = match[2];
            break;
        }

    }
    callback(null, results);
}

function forgetBrokenNode(results, callback) {
    if (!results.nodeId)
        throw 'no disconnected node found, exiting';

    yesno.ask('About to send cluster forget, ok to continue?', true, function(ok) {
        if(ok) {
            async.map(results.clusterNodes, function(node, callback) {
                if (node.status != "end") { // skip over the failed node
                    arbitraryCommand(node, 'cluster', ['forget', results.nodeId], function (err, value) {
                        if (err) throw err;
                        console.log(value.toString());
                        callback(null, null);
                    });
                } else {
                    callback(null, null);
                }
            }, function(err, data) {
                if (err) console.log(err);
                callback(null, results);
            });
        } else {
            throw 'exiting';
        }
    });
}

function addNewNode(results, callback) {
    yesno.ask('About to add new node, ok to continue?', true, function(ok) {
        if(ok) {
            arbitraryCommand(newRedis, 'cluster', ['meet', redisClusterConn.host, redisClusterConn.port], function (err, value) {
                if (err) throw err;
                console.log(value.toString());
                // TODO check that new node was successfully added before continuing. for now just pause for a second
                sleep.msleep(1000);
                callback(null, results);
            });
        } else {
            throw 'exiting';
        }
    });
}

function assignSlotsToNewNode(results, callback) {
    var args = ['addslots'];

    var slotsStr = results.missingSlots;
    var slotsChunks = slotsStr.split(",");
    for (var i in slotsChunks) {
        var slotsChunk = slotsChunks[i];

        var slotsSplit = slotsChunk.split("-");
        if (slotsSplit.length > 1) {
            for (var j = slotsSplit[0]; j <= slotsSplit[1]; j++) {
                args.push(j + "");
            }
        } else {
            args.push(slotsSplit[0]);
        }

    }

    yesno.ask("Assigning slots " + slotsStr + " to new node, ok to continue?", true, function(ok) {
        if (ok) {
            arbitraryCommand(newRedis, 'cluster', args, function (err, value) {
                if (err) throw err;
                console.log(value.toString());
                callback(null, results);
            });
        } else {
            throw 'exiting';
        }
    });
}

function arbitraryCommand(redis, command, args, callback) {
    redis.sendCommand(
        new Redis.Command(
            command,
            args,
            'utf-8',
            callback
        )
    );
}
