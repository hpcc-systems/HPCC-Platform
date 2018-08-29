import * as declare from "dojo/_base/declare";

/**
 * A decorator that converts a TypeScript class into a declare constructor.
 * This allows declare constructors to be defined as classes, which nicely
 * hides away the `declare([], {})` boilerplate.
 */
export default function (classID: string, ...mixins: Object[]) {
	return function (target: Function) {
		return declare(classID, mixins, target.prototype);
	};
}
