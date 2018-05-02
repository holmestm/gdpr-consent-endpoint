// Marketing Consent Logger - Tim Holmes - 1st May 2018

var cluster = require('cluster'),
    util = require('util'),
    winston = require('winston');
require('winston-loggly-bulk');
console.log("Welcome back my friends to the show that never ends...");
console.log(util.inspect(process.env));

var log = function(entry) {
    fs.appendFileSync('/var/log/nodejs/gdpr.log', new Date().toISOString() + ' - ' + entry + '\n');
};

// Code to run if we're in the master process
if (cluster.isMaster) {

    // Count the machine's CPUs
    var cpuCount = require('os').cpus().length;
    //log("Cluster: Starting " + cpuCount  + " workers");

    // Create a worker for each CPU
    for (var i = 0; i < cpuCount; i += 1) {
        cluster.fork();
    }

    // Listen for terminating workers
    cluster.on('exit', function (worker) {

        // Replace the terminated workers
        log('Worker ' + worker.id + ' died :(' + worker.process.pid + ')') ;
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


    AWS.config.region = process.env.REGION

    //var sns = new AWS.SNS();
    var ddb = new AWS.DynamoDB();
    var redirectURL     = process.env.REDIRECT_URL || "https://www.net-a-porter.com/en-gb/account";

    var ddbTable                = process.env.CONSENT_TABLE || "user-consent";
    //var snsTopic                = process.env.CONSENT_TOPIC || "user-consent";
    var webContext              = process.env.WEB_CONTEXT   || "/consent";
    var healthcheckContext      = process.env.HEALTH_CHECK  || "/ping";

    var app = connect();

    // extract query params if present and log them to console
    app.use((req, res, next) => {
        log("parsedURL " +  util.inspect(req._parsedUrl));
        var pathname = req._parsedUrl.pathname;
        var querystring = req._parsedUrl.query;

        if (pathname==healthcheckContext) {
            res.writeHead(200, {'Content-Type': 'text/html'});
            log("Instance health check... succeeded");
            res.end("200 Success");

        }
        if (querystring!=undefined) {
            var query = qstring.parse(querystring);
            log("Pathname [" +  pathname + "] expecting [" +  webContext + "]");
        }
        if (pathname==webContext) {
            log("Query " + util.inspect(query), true);
            req.qparams = query;
            log("Email " + req.qparams.email || "not supplied");
            next();
        }
        else {
            log("HTTP " +  res.statusCode + " : | " + pathname);
            res.writeHead(404, {'Content-Type': 'text/html'});      
            res.end("404 Not Found");
        }
    })
    
    // log query params to loggly
    app.use((req, res, next) => {
        if (req.qparams!==undefined) {
            log('Logging consent')
            log(req.qparams);
            log("Params" + util.inspect(req.qparams));
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
            log("Writing " + util.inspect(item) + " into " + ddbTable);

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

                    log("DB write error: " +  util.inspect(err));
                } 
            });
        }
        next();
    });
    
    // generate redirect
    app.use((req, res) => {
      log('Generating redirect to ' + redirectURL);
      res.statusCode = 302;
      res.setHeader("Location", redirectURL);
      res.end();
    })
    var port = process.env.PORT || 3000;

    var server = app.listen(port, function () {
        log('Starting... listening on port ' + port);
    });
}
