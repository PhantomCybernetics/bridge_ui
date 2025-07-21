import fs from "node:fs";
import z from "zod";

const booleanString = () =>
	z
		.enum(["true", "false"])
		.transform((val) => val === "true")
		.pipe(z.boolean());

// config fields that can come from env, evn vars are always strings
const envConfigSchema = z
	.object({
		UI_HTTPS: booleanString(),
		UI_HOST: z.string(),
		UI_PORT: z.coerce.number(),
		UI_PATH: z.string(),
		BRIDGE_SOCKET_URL: z.string().url(),
		BRIDGE_FILES_URL: z.string().url(),
		EXTRA_HEAD_CODE: z.array(z.string()),
	})
	.partial();

// final config schema, with default values and strict types
const fullConfigSchema = z.object({
	name: z.string().default("PHNTM BRIDGE WEB UI"),
	https: z.boolean().default(false),
	ssl: z.object({ private: z.string(), public: z.string() }).optional(),
	port: z.number().positive().int(),
	host: z.string().default("unknown"),
	path: z.string().default("/"),
	bridgeSocketUrl: z.string().url(),
	bridgeFilesUrl: z.string().url(),
	extraHeadCode: z.array(z.string()).default([]),
});

export interface BridgeRobotUiConfig {
	bridgeSocketUrl: string;
	bridgeFilesUrl: string;
	extraHeadCode: string[];
}

export interface BridgeUiConfig extends BridgeRobotUiConfig {
	path: string;
	name: string;
	https: boolean;
	port: number;
	host: string;
	ssl?: { private: string; public: string } | undefined;
}

export async function getConfig(
	configFilePath = process.env.CONFIG_FILE ?? `${__dirname}/../config.jsonc`,
): Promise<BridgeUiConfig> {
	let fileConfig: unknown = {};
	if (fs.existsSync(configFilePath)) {
		console.log("Loading config from " + configFilePath);
		fileConfig = await import(configFilePath);
	}

	const env = envConfigSchema.parse(process.env);
	const envConfigRenamed = {
		...(env.UI_HTTPS !== undefined && { https: env.UI_HTTPS }),
		...(env.UI_HOST !== undefined && { host: env.UI_HOST }),
		...(env.UI_PORT !== undefined && { port: env.UI_PORT }),
		...(env.UI_PATH !== undefined && { path: env.UI_PATH }),
		...(env.BRIDGE_SOCKET_URL !== undefined && {
			bridgeSocketUrl: env.BRIDGE_SOCKET_URL,
		}),
		...(env.BRIDGE_FILES_URL !== undefined && {
			bridgeFilesUrl: env.BRIDGE_FILES_URL,
		}),
		...(env.EXTRA_HEAD_CODE !== undefined && {
			extraHeadCode: env.EXTRA_HEAD_CODE,
		}),
	};

	return fullConfigSchema.parse(Object.assign({}, fileConfig, envConfigRenamed));
}
