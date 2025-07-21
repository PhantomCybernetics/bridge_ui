import path from "node:path";

import express, { type Express, type Request, type Response } from "express";
import ejs from "ejs";

import type { BridgeRobotUiConfig, BridgeUiConfig } from "./config";

export function printStartupMessage(
	{ uiVersion }: { uiVersion: string },
	config: BridgeUiConfig,
) {
	console.log(
		"-----------------------------------------------------------------------".yellow,
	);
	console.log(` ${config.name}`.yellow);
	console.log("");
	console.log(
		` ${config.https ? "https" : "http"}://${config.host}${
			(config.https && config.port === 443) || (!config.https && config.port === 80)
				? ""
				: `:${config.port}`
		}${config.path}__ID__`.green,
	);
	console.log((" UI version: " + uiVersion).green);
	console.log(" ".green);
	console.log(
		"----------------------------------------------------------------------".yellow,
	);
}

export function registerStaticRoutes(app: Express) {
	app.use("/static/", express.static("static/"));
	app.use("/static/socket.io/", express.static("node_modules/socket.io-client/dist/"));

	app.use("/static/gridstack/", express.static("node_modules/gridstack/dist/"));

	app.use("/static/three/", express.static("node_modules/three/build/"));
	app.use("/static/three/examples/", express.static("node_modules/three/examples/"));
	app.use("/static/urdf-loader/", express.static("node_modules/urdf-loader/src/"));

	app.use("/static/canvasjs-charts/", express.static("node_modules/@canvasjs/charts"));
	app.use("/static/touch-gamepad/", express.static("node_modules/@rbuljan/gamepad/"));
	app.get("/favicon.ico", (req: express.Request, res: Response) => {
		res.redirect("/static/favicons/favicon-yellow-16x16.png");
	});
}

export function registerViewEngine(app: Express) {
	app.engine(".html", ejs.renderFile);
	app.set("views", path.join(__dirname, "../src/views"));
	app.set("view engine", "html");
}

export function registerDummyRootRoute(app: Express) {
	app.get("/", async function (req: Request, res: Response) {
		res.setHeader("Content-Type", "text/html; charset=utf-8");
		res.send("Ohi, this is Bridge UI server");
	});
}

interface RegisterRobotUiParams extends BridgeRobotUiConfig {
	uiVersion: string;
	path: string;
}

export function registerRobotUiRoute(app: Express, params: RegisterRobotUiParams) {
	app.get(params.path + ":ID", (req: Request, res: Response) =>
		renderRobotUi(res, {
			idRobot: req.params.ID,
			...params,
		}),
	);
}

interface RenderRobotUiParams extends BridgeRobotUiConfig {
	uiVersion: string;
	idRobot: string;
}

export function renderRobotUi(res: Response, params: RenderRobotUiParams) {
	res.render("robot_ui", {
		id_robot: params.idRobot,
		bridge_socket_url: params.bridgeSocketUrl,
		bridge_files_url: params.bridgeFilesUrl,
		extra_head_code: params.extraHeadCode.join("\n"),
		ui_git_version: params.uiVersion,
	});
}
