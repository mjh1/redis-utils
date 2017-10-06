const Redis = require('ioredis');
var sleep = require('sleep');
var async = require('async');
const { StringDecoder } = require('string_decoder');
var yesno = require('yesno');


var existingRedisConn = {
    "host": "127.0.0.1",
    "port": 7001
};
var existingRedis = new Redis(existingRedisConn);
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
    arbitraryCommand(existingRedis, 'cluster', ['nodes'], function (err, value) {
        var decoder = new StringDecoder('utf8');
        var clusterInfo = decoder.write(Buffer.from(value));
        console.log("cluster info:");
        console.log(clusterInfo);
        callback(null, {
            'clusterInfo': clusterInfo
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
    console.log("~/Downloads/redis-3.2.10/src/redis-trib.rb call " + existingRedisConn.host + ":" + existingRedisConn.port + " cluster forget " + results.nodeId);
    yesno.ask('Please run forget command, ok to continue?', true, function(ok) {
        if(ok) {
            callback(null, results);
        } else {
            throw 'exiting';
        }
    });
}

function addNewNode(results, callback) {
    console.log("~/Downloads/redis-3.2.10/src/redis-trib.rb add-node " + newRedisConn.host + ":" + newRedisConn.port + " " + existingRedisConn.host + ":" + existingRedisConn.port);
    yesno.ask('Please add new node, ok to continue?', true, function(ok) {
        if(ok) {
            callback(null, results);
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
                console.log(value.toString()); //-> 'OK'
                callback(null, results);
            });
        } else {
            throw exiting;
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
