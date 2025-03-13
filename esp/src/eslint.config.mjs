import js from '@eslint/js'
import globals from 'globals'
import reactHooks from 'eslint-plugin-react-hooks'
import reactRefresh from 'eslint-plugin-react-refresh'
import tseslint from 'typescript-eslint'

export default tseslint.config(
    {
        ignores: ['dist']
    },
    {
        extends: [js.configs.recommended, ...tseslint.configs.recommended],
        files: ['**/*.{ts,tsx}'],
        languageOptions: {
            ecmaVersion: 2020,
            globals: globals.browser,
        },
        plugins: {
            'react-hooks': reactHooks,
            'react-refresh': reactRefresh,
        },
        rules: {
            "no-case-declarations": "off",
            "no-empty": "off",
            "no-empty-pattern": "off",
            "no-useless-escape": "off",
            "prefer-rest-params": "off",

            "@typescript-eslint/ban-ts-comment": [
                "error",
                {
                    "ts-expect-error": false,
                    "ts-ignore": true,
                    "ts-nocheck": true,
                    "ts-check": false
                }
            ],
            "@typescript-eslint/no-empty-interface": "off",
            "@typescript-eslint/no-empty-object-type": "off",
            "@typescript-eslint/no-explicit-any": "off",
            "@typescript-eslint/no-namespace": "off",
            "@typescript-eslint/no-this-alias": "off",
            "@typescript-eslint/no-unused-vars": "off",

            ...reactHooks.configs.recommended.rules,
            'react-refresh/only-export-components': [
                'off',
                { allowConstantExport: true },
            ],
        },
    },
)
