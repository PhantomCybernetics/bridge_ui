const startupTime:number = Date.now();

import { Debugger } from './lib/debugger';
const $d:Debugger = Debugger.Get();

import { GetCerts, UncaughtExceptionHandler } from './lib/helpers'

// includes start //

const fs = require('fs');
import * as Path from 'path';
import * as C from 'colors'; C; //force import typings with string prototype extension

import { Db, Collection, MongoError } from 'mongodb';

const _ = require('lodash');

// import { Validation as $v} from './lib/validation';

const https = require('https');

//import { AuthLib } from './lib/auth';

const mongoClient = require('mongodb').MongoClient;

// import { RouteObjectStateByKey, RouteAppCmdByKey, RouteAppStateByKey, RouteSessionStateByKey } from './lib/topicRouters';

import { ObjectId } from 'bson';
import * as SocketIO from "socket.io";

// load config & ssl certs //

const dir:string  = __dirname + "/..";

if (!fs.existsSync(dir+'/config.jsonc')) {
    $d.e('CONFIG EXPECTED AND NOT FOUND IN '+dir+'/config.jsonc');
    process.exit();
};

import * as JSONC from 'comment-json';
const defaultConfig = JSONC.parse(fs.readFileSync(dir+'/config.jsonc').toString());
const CONFIG = _.merge(defaultConfig);

const IO_PORT:number = CONFIG['BRIDGE'].ioPort;
const DB_URL:string = CONFIG.dbUrl;

const DIE_ON_EXCEPTION:boolean = CONFIG.dieOnException;

const VERBOSE:boolean = CONFIG['BRIDGE'].verbose;

const certFiles:string[] = GetCerts(dir+"/ssl/private.pem", dir+"/ssl/public.crt");
const HTTPS_SERVER_OPTIONS = {
    key: fs.readFileSync(certFiles[0]),
    cert: fs.readFileSync(certFiles[1]),
};

console.log('----------------------------------------------------------------'.yellow);
console.log(' PHNTM BRIDGE NODE'.yellow);
console.log('');
console.log((' https://localhost:'+IO_PORT+'/info                   System info').yellow);
console.log((' https://localhost:'+IO_PORT+'/robot/socket.io/       Robot API').green);
console.log((' https://localhost:'+IO_PORT+'/human/socket.io/       Human API').red);
console.log((' https://localhost:'+IO_PORT+'/app/socket.io/         App API').red);
//console.log((' Register new users via https://THIS_HOSTNAME:'+IO_PORT+'/u/r/').yellow);
console.log('----------------------------------------------------------------'.yellow);

// important global stuffs on this node defined here:
let activeUsers : { [id:number]:any } = {}; // all users active in this region
let activeLocations: { [id:number]:any } = {}; // all areas loaded and active in this region
let activeRobots: { [iRobot:number]:any } = {}; // all areas loaded and active in this region


//let knownAppKeys:string[] = [];


import * as express from "express";

const expressApp = express();
const httpServer = https.createServer(HTTPS_SERVER_OPTIONS, expressApp);
const sioRobots:SocketIO.Server = new SocketIO.Server(
    httpServer, {
        pingInterval: 10000,
        pingTimeout: 60*1000,
        path: "/robot/socket.io/"
    }
);

const sioHumans:SocketIO.Server = new SocketIO.Server(
    httpServer, {
        pingInterval: 10000,
        pingTimeout: 60*1000,
        path: "/human/socket.io/"
    }
);

expressApp.get('/', function(req: any, res: any) {
    res.setHeader('Content-Type', 'application/json');
    res.send(JSON.stringify({
        phntm_bridge: true,
        robot: '/robot/socket.io/',
        human: '/human/socket.io/',
        app: '/app/socket.io/',
    }, null, 4));
});

expressApp.get('/info', function(req: any, res: any) {
    res.setHeader('Content-Type', 'application/json');
    res.send(JSON.stringify({info:"todo"}, null, 4));
});


httpServer.listen(IO_PORT);
$d.l('Server listening on port '+IO_PORT);


// Robot Socket.io

sioRobots.on('connect', function(socket : SocketIO.Socket){

    $d.log('Ohai robot! Opening Socket.io for', socket.handshake.address);

    //$d.log(socket);

    /*let user : User = new User(socket);
    user.isConnected = true;
    user.regionPartition = REGION_PARTITION;
    user.isAuthentificated = false;

    user.idSession = null; //generated on login
    user.shortUserId = 0; //generated on login, short sess pass / id

    //init cave fix to zero
    //user.caveFix = mat4.create(); mat4.identity(user.caveFix);

    socket.user = user;
    */
    /*
     * client auth
     */
    socket.on('auth', function(data, returnCallback) {

        $d.log('Got robot auth request', data);

        returnCallback(({'err':'lolo'}));

        /*if (user.loginInProgress) {
            return returnCallback({ res: 0, msg: 'Another login in progress' });
        }

        if (data && !data.clientVersion) {
            return returnCallback({ res: 0, msg: 'Client version not provided' });
        } else if (data && data.clientVersion && data.clientVersion < 0.1) {
            if (data['password']) { data['password'] = '****'; } //safe logs
            $d.err("Invalid client version received from "+user.clientAddress+" with data", data);
            return returnCallback({ res: 0, msg: "Unsupported client version detected, update Phantom via the App Store!\n(consider automatic updates for better experience)" });
        }

        if (data) {

            if ($v.isSet(data.clientType)) { user.clientType = data.clientType; }
            if ($v.isSet(data.deviceType)) { user.deviceType = data.deviceType; }

            if (!data.gps) {
                $d.err("Client GPS coords not provided");
                return returnCallback( { 'res':0, 'msg':'No GPS coordinates provided'} );
            }
            if (!data.gpsAccuracy || !$v.isSet(data.gpsAccuracy[0]) || !$v.isSet(data.gpsAccuracy[1])) {
                $d.err("Gps accuracy not provided or invalid", data.gpsAccuracy);
                return returnCallback( { 'res':0, 'msg':'No or invalid GPS accuracy provided'} );
            }
            user.lastGps = new Gps(data.gps[0], data.gps[1], data.gps[2]);
            user.gpsAccuracy = data.gpsAccuracy[0];

            if (!data.northVector) {
                $d.err("Session north vector not provided");
                return returnCallback( { 'res':0, 'msg':'No session north vector provided'} );
            }
            user.northVector = vec3.fromValues(data.northVector[0], data.northVector[1], data.northVector[2]);
            if (!$v.isSet(data.compassAccuracy)) {
                $d.err("Compass accuracy not provided");
                return returnCallback( { 'res':0, 'msg':'No compass accuracy provided'} );
            }
            user.compassAccuracy = data.compassAccuracy;

            if (!$v.isSet(data.deviceSensorOffset)) {
                $d.err("Device sensor offset not provided");
                return returnCallback( { 'res':0, 'msg':'Device sensor offset not provided'} );
            }
            user.deviceSensorOffset = data.deviceSensorOffset;

            $d.log('User GPS is: '.gray+(user.lastGps? user.lastGps.toString():'null').yellow+(' (h-acc='+data.gpsAccuracy[0]+'); ' +
                   'north vector: ['+ArrayToFixed(user.northVector)+'] (compass acc='+user.compassAccuracy+')').gray);

            //try session id login
            if ($v.isSet(data.sessionCookie)) {
                user.loginInProgress = true;

                return AuthLib.LoginWithSessionCookieAsync( //calls returnCallback with { res: 0 } or { res: 1, userData: {} }
                    data.sessionCookie, user, activeUsers,
                    worldDb, appsDb,
                    kafkaProducer, activeSessions, STATIC_SERVER_ADDRESS, returnCallback, VERBOSE,
                    () => { //onSuccess when session is created:
                        //SessionHelpers.LocalizeEveryoneFollowingUser(user.idUser, activeSessions, activeUsers, sessionObjects, appObjects, loadingAppObjectBatches, kafkaProducer);
                    }
                );
            }

            //anonymous but unique device id
            else if ($v.isSet(data.idDevice) && data.idDevice) {
                user.loginInProgress = true;
                return AuthLib.AnonymousLoginWithIdDeviceAsync ( //calls returnCallback with { res: 0 } or { res: 1, userData: {} }
                    data.idDevice, user, DEFAULT_USER_APP_KEYS, activeUsers,
                    worldDb, appsDb,
                    kafkaProducer, activeSessions, STATIC_SERVER_ADDRESS, returnCallback, VERBOSE,
                    () => { //onSuccess after session is created
                        //SessionHelpers.LocalizeEveryoneFollowingUser(user.idUser, activeSessions, activeUsers, sessionObjects, appObjects, loadingAppObjectBatches, kafkaProducer);
                    }
                );
            }

            //credentials
            else if ($v.isSet(data.handle) && $v.isSet(data.password) && data.handle && data.password) {
                user.loginInProgress = true;
                return AuthLib.LoginWithCredentialsAsync ( //calls returnCallback with { res: 0 } or { res: 1, userData: {} }
                    data.handle, data.password, user, activeUsers,
                    worldDb, appsDb,
                    kafkaProducer, activeSessions, STATIC_SERVER_ADDRESS, returnCallback, VERBOSE,
                    () => { //onSuccess after session is created
                        //SessionHelpers.LocalizeEveryoneFollowingUser(user.idUser, activeSessions, activeUsers, sessionObjects, appObjects, loadingAppObjectBatches, kafkaProducer);
                    }
                );
            }
        }

        //user log out (but still connected)
        else if (!data && user) {

            $d.log((user+' logged out from '+user.clientAddress).blue);

            SessionHelpers.ClearUser(user, activeUsers, activeSessions, activeAreas, sessionObjects, activeObjects, kafkaProducer, VERBOSE);

            //loged out but not disconnected - make a new session obj bcs the old one is destroyed
            user = new User(socket);
            user.isConnected = true;
            user.regionPartition = REGION_PARTITION;
            user.isAuthentificated = false;
            user.idSession = new ObjectID().toHexString();

            //init cave fix to zero
            //user.caveFix = mat4.create(); mat4.identity(user.caveFix);

            socket.user = user;

            returnCallback({ res: 1 });

            return;

        }
        */
    });

    /*
     * client disconnected
     */
    socket.on('disconnect', (data:any) => {

        $d.l(('Socket disconnect for robot: '+data).red);

        /*if (user != null && user.clientType == ClientType.PHNTM) {
            $d.log((user+' at '+user.clientAddress+' disconnected').blue);
            SessionHelpers.ClearUser(user, activeUsers, activeSessions, activeAreas, sessionObjects, activeObjects, kafkaProducer, VERBOSE);

        } else if (user != null) {
            $d.log((NodeTypeToName(user.clientType, [ user.regionPartition ]) +' at '+user.clientAddress+' disconnected'));
        }*/

        //SessionHelpers.ClientDisconnectHandler(user, activeUsers, activeSessions, sessionObjects, activeObjects, kafkaProducer);
    });

    socket.on('disconnecting', (reason:any) => {
        $d.l(('Socket disconnecting from robot: '+reason).gray);
    });






});











//error handling & shutdown
process.on('uncaughtException', (err:any) => {
    UncaughtExceptionHandler(err, false);
    if (DIE_ON_EXCEPTION) {
        _Clear();
        ShutdownWhenClear();
    }
} );

process.on('SIGINT', (code:any) => {
    $d.log("Worker exiting...");
    _Clear();
    ShutdownWhenClear();
});

let shuttingDown:boolean = false;
//let cleanupTimer:NodeJS.Timeout = null;

function _Clear() {
    if (shuttingDown) return;
    shuttingDown = true;

    $d.log("Cleaning up...");

    sioRobots.close();
    sioHumans.close();
    //clearInterval(cleanupTimer);
    //_SaveAndClearAbandonedSessions(); //will wait until areas and sessions clear
}

function ShutdownWhenClear() {
    /*if ((consumerWrapper && consumerWrapper.connected)
        || ObjectSize(activeSessions)
        || ObjectSize(activeAreas)
    ) {
        return setTimeout(ShutdownWhenClear, 10);
    }

    if (kafkaProducerConnected) {
        kafkaProducer.disconnect();
    }

    if (kafkaProducerConnected) {
        return setTimeout(ShutdownWhenClear, 10);
    }*/

    process.exit(0);
}