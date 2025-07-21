import fs from "node:fs";
import https from "node:https";
import http from "node:http";

import express from "express";
import C from "colors";
C; //force import typings with string prototype extension

import { getConfig } from "./config.ts";
import { Debugger } from "./lib/debugger.ts";
import { GetCerts } from "./lib/helpers.ts";
import {
	printStartupMessage,
	registerDummyRootRoute,
	registerStaticRoutes,
	registerRobotUiRoute,
	registerViewEngine,
} from "./WebUIServer";

const $d = Debugger.Get();

const config = await getConfig();
const uiVersion = (await import("../package.json")).version;

printStartupMessage({ uiVersion }, config);

const app = express();
registerStaticRoutes(app);
registerDummyRootRoute(app);
registerViewEngine(app);
registerRobotUiRoute(app, { uiVersion, ...config });

function httpsOptions() {
	const SSL_CERT_PRIVATE = config.ssl!.private;
	const SSL_CERT_PUBLIC = config.ssl!.public;
	const certFiles: string[] = GetCerts($d, SSL_CERT_PRIVATE, SSL_CERT_PUBLIC);
	return {
		key: fs.readFileSync(certFiles[0]),
		cert: fs.readFileSync(certFiles[1]),
	};
}

const webHttpServer = config.https
	? https.createServer(httpsOptions(), app)
	: http.createServer(app);

webHttpServer.listen(config.port);
console.log(
	`${config.https ? "HTTPS" : "HTTP"} server listening on port ${config.port}`.green,
);
