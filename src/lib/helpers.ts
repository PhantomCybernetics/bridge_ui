import type { Debugger } from "./debugger";

export function GetCerts($d: Debugger, priv: string, pub: string): string[] {
	let certFiles: string[] = [priv, pub];
	const fs = require("fs");
	for (var i = 0; i < 2; i++) {
		if (!fs.existsSync(certFiles[i])) {
			$d.log(
				(
					certFiles[i] +
					" not found. Run `sh ./ssl/gen.sh` to generate a self signed SSL certificate"
				).red,
			);
			break;
		}
	}
	return certFiles;
}
