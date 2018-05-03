// Marketing Consent Logger - Tim Holmes - 3rd May 2018 - v5.5

var cluster = require('cluster'),
    util = require('util'),
    url = require('url'),
    winston = require('winston');
require('winston-loggly-bulk');

var argv = require('minimist')(process.argv.slice(2), 
    {default : {redirectURL: 'https://www.net-a-porter.com/en-gb/account', 
                logglySubdomain: "DISABLED", 
                logglyToken: "DISABLED",
                logglyTags: "pacman|gdpr",
                consentTable: "DISABLED",
                consentTopic: "DISABLED",
                context: "/consent",
                logLevel: "debug",
                ddbTable: "DISABLED",
                snsTopic: "DISABLED",
                redirect: true,
                debug: true,
                port: 8081,
                logfile: '/var/log/nodejs/gdpr.log'},
    alias   : {p : 'port'},
    boolean : ["redirect"]});

    if ((typeof argv.redirect)=="string") argv.redirect=argv.redirect!=="false";

if (cluster.isMaster && argv.debug) {
    console.log("Welcome back my friends to the show that never ends...");
    console.log("--- args");
    console.dir(argv);
    console.log("--- env");
    console.dir(process.env);
}

var redirectURL     = process.env.REDIRECT_URL || argv.redirectURL;
var ddbTable        = process.env.CONSENT_TABLE || argv.consentTable;
var snsTopic        = process.env.CONSENT_TOPIC || argv.consentTopic;
var webContext      = process.env.WEB_CONTEXT || argv.context;
var secret          = process.env.SIGNATURE_SECRET || argv.secret || "signature-secret-22264";
var logglyToken     = process.env.LOGGLY_TOKEN || argv.logglyToken;
var logglySubdomain = process.env.LOGGLY_SUBDOMAIN || argv.logglySubdomain;
var logglyTags      = process.env.LOGGLY_TAGS || argv.logglyTags;
var logLevel        = process.env.LOG_LEVEL || argv.logLevel || "debug";
var logfile         = process.env.LOG_FILE || argv.logfile || "NONE";

winston.handleExceptions([ winston.transports.Console]);
if (logfile!==undefined && logfile!=="NONE"){
    winston.add(winston.transports.File, {
        filename: logfile,
        handleExceptions: true,
        json: false
    })
}
if (logglySubdomain!==undefined && logglySubdomain!="DISABLED") {
    winston.add(winston.transports.Loggly, {
        inputToken: logglyToken,
        subdomain: logglySubdomain,
        tags: logglyTags.split("|"),
        json: true,
        handleExceptions: true,
        level: logLevel
})}
winston.level = logLevel;
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
        connect = require('connect'),
        qstring = require('qs'),
        sign = require('sign-payload'),
        _ = require('underscore');

    var debug = (x) => { winston.debug(x, { pid: cluster.worker.process.pid})}
    var info  = (x) => { winston.info(x, { pid: cluster.worker.process.pid})}
    var error = (x) => { winston.error(x, { pid: cluster.worker.process.pid})}
    AWS.config.region = process.env.REGION || argv.region;

    var sns = new AWS.SNS();
    var ddb = new AWS.DynamoDB();
    
    var ddbDisabled = ddbTable == "DISABLED";
    var snsDisabled = snsTopic == "DISABLED";

    debug(`Worker ${cluster.worker.id}`);

    var app = connect();

    var httpError = (statusCode, req, res, message) => {
        winston.info(`HTTP ${statusCode} : |${url.parse(req.url).pathname}| `);
        res.writeHead(statusCode, {'Content-Type': 'text/html'});      
        res.end(`${statusCode} - ${message}`);
    }

    // extract query params if present and log them to console
    // reject any URLs that don't use our context and return a 404
    app.use((req, res, next) => {
        var parsedUrl = url.parse(req.url);
        debug(`parsedUrl ${util.inspect(req.url)}`);
        var pathname = parsedUrl.pathname;
        var querystring = parsedUrl.query;
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
            httpError(404, req, res, "Not Found");
        }
    })

    // validate parameters using supplied signature
    app.use((req, res, next)=> {
        if (secret!=="DISABLED") {
            var { email, uid, sig } = req.qparams;
            var [alg, signature]=sign(email+"|"+uid, secret).split('=');
            if (sig===undefined || sig!=signature) {
                error(`Signature not supplied or mismatch ${alg}=${signature} !== ${sig||"(not supplied)"}`);
                httpError(400, req, res, "Bad Request - Invalid Signature");
            } else {
                next();
            }
        } else {
            next();
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

    // add query params to DynamoDB + raise event via SNS if configured
    app.use((req, res, next) => {
        var { email, uid, sig } = req.qparams;
        if (email!==undefined && uid!==undefined) {
            var d = (new Date()).toJSON();
            var item = {
                'email': {'S': email},
                'uid': {'S': uid},
                'sig': {'S': sig || "NOT_SUPPLIED"},
                'timestamp': {'S': d}
            };

            if (!ddbDisabled) {
                debug("Persisting consent " + util.inspect(item) + " into " + ddbTable);
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
                    } else if (!snsDisabled) {

                        // Raise event via SNS

                        debug("Written to DB now raising SNS event");
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
                })
            } else {
                debug("Not Writing " + util.inspect(item));                
            }
        }
        else {
            debug("Email and UID not present " + util.inspect(req.qparams));                
        }
        next();
    });
    
    // generate redirect
    app.use((req, res) => {
        var redirect=argv.redirect;
        debug(`${redirect} ${argv.redirect} ${util.inspect(req.qparams)} ${req.qparams.redirect}`)
        if (req.qparams!==undefined && req.qparams.redirect!==undefined) {
            redirect=req.qparams.redirect==="true";
        }
        debug(`2 ${redirect} ${typeof redirect}`)
        if (redirect) {
            debug('Generating redirect to ' + redirectURL);
            res.statusCode = 302;
            res.setHeader("Location", redirectURL);
            res.end();
        } else {
            debug('Not generating redirect to ' + redirectURL);
            res.writeHead(200, {'Content-Type': 'text/html'});      
            res.write(`<html><body><a href="${redirectURL}">Thankyou for your consent</a></body></html>`);
            res.end();         
        }
    })
    var port = process.env.PORT || argv.port || 8081;

    var server = app.listen(port, function () {
        debug(`Starting worker ${cluster.worker.id} listening on port ` + port);
    });
}