// Marketing Consent Logger - Tim Holmes - 1st May 2018

var cluster = require('cluster'),
    util = require('util'),
    winston = require('winston');
require('winston-loggly-bulk');
console.log("Welcome back my friends to the show that never ends...");
console.log(util.inspect(process.env));

var logglyToken     = process.env.LOGGLY_TOKEN || "5f81877a-15b9-43b1-8d45-8d4b1c204a42";
var logglySubdomain = process.env.LOGGLY_SUBDOMAIN || "gravitaz";
var logglyTags      = process.env.LOGGLY_TAGS || "pacman|gdpr";
var logLevel        = process.env.LOG_LEVEL || "info";

winston.add(winston.transports.Loggly, {
    inputToken: logglyToken,
    subdomain: logglySubdomain,
    tags: logglyTags.split("|"),
    json: true,
    level: logLevel
});

// Code to run if we're in the master process
if (cluster.isMaster) {

    // Count the machine's CPUs
    var cpuCount = require('os').cpus().length;
    winston.debug(`Cluster: Starting ${cpuCount} workers`);

    // Create a worker for each CPU
    for (var i = 0; i < cpuCount; i += 1) {
        cluster.fork();
    }

    // Listen for terminating workers
    cluster.on('exit', function (worker) {

        // Replace the terminated workers
        winston.error('Worker ' + worker.id + ' died :(', { pid: worker.process.pid });
        cluster.fork();

    });

// Code to run if we're in a worker process
} else {
    var AWS = require('aws-sdk'),
        util = require('util'),
        http = require('http'),
        https = require('https'),
        connect = require('connect'),
        colors = require('colors'),
        fs = require('fs'),
        qstring = require('qs'),
        _ = require('underscore');


    var debug = (x) => { winston.debug(x, { pid: cluster.worker.process.pid})}
    var info  = (x) => { winston.info(x, { pid: cluster.worker.process.pid})}
    var error = (x) => { winston.error(x, { pid: cluster.worker.process.pid})}
    AWS.config.region = process.env.REGION

    var sns = new AWS.SNS();
    var ddb = new AWS.DynamoDB();
    var redirectURL     = process.env.REDIRECT_URL || "https://www.net-a-porter.com/en-gb/account";

    var ddbTable        = process.env.CONSENT_TABLE || "user-consent";
    var snsTopic        = process.env.CONSENT_TOPIC || "user-consent";
    var webContext      = process.env.WEB_CONTEXT || "/consent";

    var app = connect();

    // extract query params if present and log them to console
    app.use((req, res, next) => {
        debug(`_parsedUrl ${util.inspect(req._parsedUrl)}`);
        var pathname = req._parsedUrl.pathname;
        var querystring = req._parsedUrl.query;
        if (querystring!=undefined) {
            var query = qstring.parse(querystring);
            debug(`Pathname ${pathname} expecting ${webContext}`);
        }
        if (pathname==webContext) {
            debug("Query " + util.inspect(query), true);
            req.qparams = query;
            debug("Email " + req.qparams.email || "not supplied");
            next();
        }
        else {
            winston.info(`HTTP ${res.statusCode} : |${pathname}| `);
            res.writeHead(404, {'Content-Type': 'text/html'});      
            res.end("404 Not Found");
        }
    })
    
    // log query params to loggly
    app.use((req, res, next) => {
        if (req.qparams!==undefined) {
            debug('Logging consent')
            info(req.qparams);
            debug("Params" + util.inspect(req.qparams));
        };
        next();
    })

    // add query params to DynamoDB
    app.use((req, res, next) => {
        var { email, uid } = req.qparams;
        if (email!==undefined && uid!==undefined) {
            var d = (new Date()).toJSON();
            var item = {
                'email': {'S': email || "undefined@gravitaz.co.uk"},
                'uid': {'S': uid || ""},
                'timestamp': {'S': d}
            };
            debug("Writing " + util.inspect(item) + " into " + ddbTable);

            ddb.putItem({
                'TableName': ddbTable,
                'Item': item,
                'Expected': { 'email': { Exists: false } }        
            }, function(err, data) {
                if (err) {
                    var returnStatus = 500;

                    if (err.code === 'ConditionalCheckFailedException') {
                        returnStatus = 409;
                    }

                    winston.debug(`HTTP ${returnStatus} : ${util.inspect(err)} `);
                } else {

                    // Raise event via SNS

                    debug("Written to DynamoDB now raising SNS event");
                    sns.publish({
                        'Message': util.inspect(item),
                        'Subject': 'New consent given',
                        'TopicArn': snsTopic
                    }, function(err, data) {
                        if (err) {
                            error('SNS Error: ' + err);
                        } else {
                            debug('SNS Event Raised ' + util.inspect(data));
                        }
                    });            
                }
            });
        }
        next();
    });
    
    // generate redirect
    app.use((req, res) => {
      debug('Generating redirect to ' + redirectURL);
      res.statusCode = 302;
      res.setHeader("Location", redirectURL);
      res.end();
    })
    var port = process.env.PORT || 3000;

    var server = app.listen(port, function () {
        debug(`Starting... listening on port ` + port);
    });
}