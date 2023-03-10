require('dotenv').config();

var fs = require('fs');
var path = require('path');
var os = require('os');
var cluster = require('cluster');
var mysql = require('mysql');
var async = require('async');
var extend = require('extend');
var chalk = require('chalk');
var redis = require('redis');

var CliListener = require('./libs/cliListener.js');
var PoolWorker = require('./libs/poolWorker.js');

var yiimplib = require('./libs/yiimplib.js');
var algos = require('stratum-pool/lib/algoProperties.js');
var mpos = require('./libs/mposCompatibility.js');
var poolWorkers = {};
var pid = process.pid;

JSON.minify = JSON.minify || require('node-json-minify');

if (!fs.existsSync('config.json')) {
    console.log(
        'config.json file does not exist. Read the installation/setup instructions.'
    );
    return;
}

var portalConfig = JSON.parse(
    JSON.minify(fs.readFileSync('config.json', { encoding: 'utf8' }))
);

var poolConfigs;
const pino = require('pino')
//const dest = pino.extreme() // logs to stdout with no args
const logger = pino({
    colorize: chalk.supportsColor,
    translateTime: true,
    ignore: 'pid,hostname',
    prettyPrint: {
       levelFirst: true
    },
    prettifier: require('pino-pretty'),
    processors: ['level', 'time', 'message']
});

//Try to give process ability to handle 100k concurrent connections
try {
    var posix = require('posix');
    try {
        posix.setrlimit('nofile', { soft: 100000, hard: 100000 });
    } catch (e) {
        if (cluster.isMaster)
            logger.warn(
                'POSIX',
                'Connection Limit',
                '(Safe to ignore) Must be ran as root to increase resource limits'
            );
    } finally {
        // Find out which user used sudo through the environment variable
        var uid = parseInt(process.env.SUDO_UID);
        // Set our server's uid to that user
        if (uid) {
            process.setuid(uid);
            logger.debug(
                'POSIX',
                'Connection Limit',
                'Raised to 100K concurrent connections, now running as non-root user: ' +
                    process.getuid()
            );
        }
    }
} catch (e) {
    if (cluster.isMaster)
        logger.debug(
            'POSIX',
            'Connection Limit',
            '(Safe to ignore) POSIX module not installed and resource (connection) limit was not raised'
        );
}

// ======================================================
// ---THIS IS WHERE WE BROADCAST FOR MASTER -> WORKER-------
//  =======================================================
if (cluster.isMaster) {
    yiimpTimer();
}

function yiimpTimer() {
    setInterval(function(id) {
        if (poolWorkers) {
            tmpWorkerCount = 0;
            totalMinerCount = 0
            for (var key in poolWorkers){
                poolWorkers[key].send({type: 'getMinerCount'});
            }
            poolWorkers[0].send({ type: 'yiimpTimer' });
        }
    }, 15000);
}



// ======================================================
// ---THIS IS WHERE WE LISTEN FOR MASTER -> WORKER-------
//  =======================================================
if (cluster.isWorker) {
    switch (process.env.workerType) {
        case 'pool':
            // If we are currently in a forker pool worker, run this
            new PoolWorker(logger);
            break;
    }

    return;
}

//Read all pool configs from pool_configs and join them with their coin profile
var buildPoolConfigs = function() {
    var configs = {};
    var configDir = 'pool_configs/';

    var poolConfigFiles = [];

    /* Get filenames of pool config json files that are enabled */
    fs.readdirSync(configDir).forEach(function(file) {
        if (
            !fs.existsSync(configDir + file) ||
            path.extname(configDir + file) !== '.json'
        )
            return;
        var poolOptions = JSON.parse(
            JSON.minify(
                fs.readFileSync(configDir + file, { encoding: 'utf8' })
            )
        );
        if (!poolOptions.enabled) return;
        poolOptions.fileName = file;
        poolConfigFiles.push(poolOptions);
    });

    /* Ensure no pool uses any of the same ports as another pool */
    for (var i = 0; i < poolConfigFiles.length; i++) {
        var ports = Object.keys(poolConfigFiles[i].ports);
        for (var f = 0; f < poolConfigFiles.length; f++) {
            if (f === i) continue;
            var portsF = Object.keys(poolConfigFiles[f].ports);
            for (var g = 0; g < portsF.length; g++) {
                if (ports.indexOf(portsF[g]) !== -1) {
                    logger.error(
                        'Master',
                        poolConfigFiles[f].fileName,
                        'Has same configured port of ' +
                            portsF[g] +
                            ' as ' +
                            poolConfigFiles[i].fileName
                    );
                    process.exit(1);
                    return;
                }
            }

            if (poolConfigFiles[f].coin === poolConfigFiles[i].coin) {
                logger.error(
                    'Master',
                    poolConfigFiles[f].fileName,
                    'Pool has same configured coin file coins/' +
                        poolConfigFiles[f].coin +
                        ' as ' +
                        poolConfigFiles[i].fileName +
                        ' pool'
                );
                process.exit(1);
                return;
            }
        }
    }

    poolConfigFiles.forEach(function(poolOptions) {
        poolOptions.coinFileName = poolOptions.coin;

        var coinFilePath = 'coins/' + poolOptions.coinFileName;
        if (!fs.existsSync(coinFilePath)) {
            logger.error(
                'Master',
                poolOptions.coinFileName,
                'could not find file: ' + coinFilePath
            );
            return;
        }

        var coinProfile = JSON.parse(
            JSON.minify(fs.readFileSync(coinFilePath, { encoding: 'utf8' }))
        );
        poolOptions.coin = coinProfile;
        poolOptions.coin.name = poolOptions.coin.name.toLowerCase();

        if (poolOptions.coin.name in configs) {
            logger.error(
                'Master',
                poolOptions.fileName,
                'coins/' +
                    poolOptions.coinFileName +
                    ' has same configured coin name ' +
                    poolOptions.coin.name +
                    ' as coins/' +
                    configs[poolOptions.coin.name].coinFileName +
                    ' used by pool config ' +
                    configs[poolOptions.coin.name].fileName
            );

            process.exit(1);
            return;
        }

        for (var option in portalConfig.defaultPoolConfigs) {
            if (!(option in poolOptions)) {
                var toCloneOption = portalConfig.defaultPoolConfigs[option];
                var clonedOption = {};
                if (toCloneOption.constructor === Object)
                    extend(true, clonedOption, toCloneOption);
                else clonedOption = toCloneOption;
                poolOptions[option] = clonedOption;
            }
        }

        configs[poolOptions.coin.name] = poolOptions;

        if (!(coinProfile.algorithm in algos)) {
            logger.error(
                'Master',
                coinProfile.name,
                'Cannot run a pool for unsupported algorithm "' +
                    coinProfile.algorithm +
                    '"'
            );
            delete configs[poolOptions.coin.name];
        }
    });
    return configs;
};

function roundTo(n, digits) {
    if (digits === undefined) {
        digits = 0;
    }
    var multiplicator = Math.pow(10, digits);
    n = parseFloat((n * multiplicator).toFixed(11));
    var test = Math.round(n) / multiplicator;
    return +test.toFixed(digits);
}

var _lastStartTimes = [];
var _lastShareTimes = [];

var spawnPoolWorkers = function() {
    var redisConfig;
    var connection;

    Object.keys(poolConfigs).forEach(function(coin) {
        var pcfg = poolConfigs[coin];
        if (!Array.isArray(pcfg.daemons) || pcfg.daemons.length < 1) {
            logger.error(
                'Master',
                coin,
                'No daemons configured so a pool cannot be started for this coin.'
            );
            delete poolConfigs[coin];
        }
    });

    if (Object.keys(poolConfigs).length === 0) {
        logger.warn(
            'Master',
            'PoolSpawner',
            'No pool configs exists or are enabled in pool_configs folder. No pools spawned.'
        );
        return;
    }

    var serializedConfigs = JSON.stringify(poolConfigs);

    var numForks = (function() {
        if (!portalConfig.clustering || !portalConfig.clustering.enabled)
            return 1;
        if (portalConfig.clustering.forks === 'auto') return os.cpus().length;
        if (
            !portalConfig.clustering.forks ||
            isNaN(portalConfig.clustering.forks)
        )
            return 1;
        return portalConfig.clustering.forks;
    })();

    //var poolWorkers = {};

    var createPoolWorker = function(forkId) {
        var worker = cluster.fork({
            workerType: 'pool',
            pid: process.pid,
            forkId: forkId,
            pools: serializedConfigs,
            portalConfig: JSON.stringify(portalConfig),
	});
	    console.log("createPoolWorker pid=" + process.pid + " forkId:" + forkId);
        worker.forkId = forkId;
        worker.type = 'pool';
        var redisCommands = [];
        // var lastShareSubmitTime = 0;
        // var pileUp = 0;

        poolWorkers[forkId] = worker;

        worker
            .on('exit', function(code, signal) {
                logger.error(
                    'Master',
                    'PoolSpawner',
                    'Fork ' + forkId + ' died, spawning replacement worker...'
                );
                setTimeout(function() {
                    createPoolWorker(forkId);
                }, 2000);
            })
            .on('message', function(msg) {
                switch (msg.type) {
                    case 'banIP':
                        Object.keys(cluster.workers).forEach(function(id) {
                            if (cluster.workers[id].type === 'pool') {
                                cluster.workers[id].send({
                                    type: 'banIP',
                                    ip: msg.ip,
                                });
                            }
                        });
                        break;
		 case 'minerCount':
                        tmpWorkerCount++;
                        totalMinerCount += msg.minerCount;
                        if (tmpWorkerCount == Object.keys(poolWorkers).length){
                                yiimplib.refresh(logger, totalMinerCount);
				// console.log("Total Miner Count: " + totalMinerCount);
                        }
                        break;
                }
            });
    };

    var i = 0;
    var spawnInterval = setInterval(function() {
        createPoolWorker(i);
        i++;
        if (i === numForks) {
            clearInterval(spawnInterval);
            logger.debug(
                'Master',
                'PoolSpawner',
                'Spawned ' +
                    Object.keys(poolConfigs).length +
                    ' pool(s) on ' +
                    numForks +
                    ' thread(s)'
            );
        }
    }, 250);
};

var startCliListener = function() {
    var cliPort = portalConfig.cliPort;

    var listener = new CliListener(cliPort);
    listener
        .on('log', function(text) {
            logger.debug('Master', 'CLI', text);
        })
        .on('command', function(command, params, options, reply) {
            switch (command) {
                case 'blocknotify':
                    Object.keys(cluster.workers).forEach(function(id) {
                        cluster.workers[id].send({
                            type: 'blocknotify',
                            coin: params[0],
                            hash: params[1],
                        });
                    });
                    reply('Pool workers notified');
                    break;
                case 'coinswitch':
                    processCoinSwitchCommand(params, options, reply);
                    break;
                case 'reloadpool':
                    Object.keys(cluster.workers).forEach(function(id) {
                        cluster.workers[id].send({
                            type: 'reloadpool',
                            coin: params[0],
                        });
                    });
                    reply('reloaded pool ' + params[0]);
                    break;
                default:
                    reply('unrecognized command "' + command + '"');
                    break;
            }
        })
        .start();
};


module.exports = function(logger, poolConfig) {
    const mposConfig = poolConfig.mposMode;
    const coin = poolConfig.coin.name;

    const connection = mysql.createPool({
        host: mposConfig.host,
        port: mposConfig.port,
        user: mposConfig.user,
        password: mposConfig.password,
        database: mposConfig.database,
    });

    const query = (...args) => {
        return new Promise((resolve, reject) => {
            console.log(`Running query ${args[0]} with data ${args[1]}`);
            connection.query(args[0], args[1], function(err, result) {
                if (err) reject(err);
                resolve(result);
            });
        });
    };
};

(function init() {
    poolConfigs = buildPoolConfigs();

    spawnPoolWorkers();

    startCliListener();
})();
