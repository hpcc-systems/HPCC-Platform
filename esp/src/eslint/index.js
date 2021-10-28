// eslint-disable-next-line no-undef
module.exports = {
    rules: {
        "no-src-react": {
            create: function (context) {
                return {
                    ImportDeclaration(node) {
                        if (node && node.source && node.source.value && node.source.value.indexOf("src-react/") === 0) {
                            context.report({
                                node,
                                message: "Prefer '..' to 'src-react'",
                                fix: function (fixer) {
                                    return fixer.replaceText(node.source, node.source.raw.replace("src-react/", "../"));
                                }
                            });
                        }
                    }
                };
            }
        }
    }
};