// eslint-disable-next-line no-undef
module.exports = {
    root: true,
    parser: "@typescript-eslint/parser",
    plugins: [
        "@typescript-eslint",
    ],
    extends: [
        "eslint:recommended",
        "plugin:@typescript-eslint/recommended",
    ],
    env: {
        "browser": true,
        "amd": true
    },
    globals: {
        "dojo": "readonly",
        "dijit": "readonly",
        "dojoConfig": "readonly",
        "debugConfig": "readonly",
        "Promise": "readonly"
    },
    rules: {
        "no-redeclare": "off",
        "no-empty": "off",
        "no-empty-pattern": "off",
        "no-constant-condition": "off",
        "no-case-declarations": "off",
        "no-prototype-builtins": "off",
        "no-unused-vars": "off",
        "no-useless-escape": "off",
        "no-unexpected-multiline": "off",
        "no-extra-boolean-cast": "off",
        "no-self-assign": "off",
        "no-multiple-empty-lines": [
            "error", {
                max: 1
            }
        ],

        "func-call-spacing": ["error", "never"],
        "space-before-function-paren": ["error", {
            "anonymous": "always",
            "named": "never",
            "asyncArrow": "always"
        }],
        "comma-spacing": [
            "error", {
                "before": false,
                "after": true
            }
        ],

        "prefer-rest-params": "off",
        "prefer-spread": "off",

        "semi": ["error", "always"],
        "quotes": [
            "error",
            "double", {
                "avoidEscape": true
            }
        ],

        "@typescript-eslint/explicit-module-boundary-types": "off",
        "@typescript-eslint/no-unused-vars": "off",
        "@typescript-eslint/ban-ts-comment": "off",
        "@typescript-eslint/no-inferrable-types": "off",
        "@typescript-eslint/no-empty-function": "off",
        "@typescript-eslint/no-explicit-any": "off",
        "@typescript-eslint/no-empty-interface": "off",
        "@typescript-eslint/no-this-alias": "off",
        "@typescript-eslint/ban-types": [
            "error",
            {
                "types": {
                    // add a custom message, AND tell the plugin how to fix it
                    "String": {
                        "message": "Use string instead",
                        "fixWith": "string"
                    },

                    "{}": {
                        "message": "Use object instead",
                        "fixWith": "object"
                    },

                    "object": false
                }
            }
        ],
        "@typescript-eslint/no-non-null-assertion": "off",
        "@typescript-eslint/no-namespace": "off",
        "@typescript-eslint/no-var-require": "off"
    }
};
