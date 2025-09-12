import { defineConfig } from "vitepress";
import { observable } from "@hpcc-js/markdown-it-plugins";
import { eclLang } from "@hpcc-js/markdown-it-plugins/ecl-lang";

// https://vitepress.dev/reference/site-config
export default async () => {

    return defineConfig({
        title: 'HPCC Platform',
        description: 'The HPCC-Platform from hpccsystems is an open source system for big data analysis. It uses a single language, platform and architecture to process data efficiently and fast.',
        base: '/HPCC-Platform/',
        srcDir: '..',
        srcExclude: [".*/**", "build-*/**", "build/**", "docs/blogs/**", "esp/**", "node_modules/**", "third-party/**","vcpkg/*/**", "plugins/**", "initfiles/examples/ResultCompare/**", "helm/**", "dockerfiles/**"],
        ignoreDeadLinks: true,

        themeConfig: {
            search: {
                provider: 'local'
            },
            editLink: {
                pattern: 'https://github.com/hpcc-systems/HPCC-Platform/edit/master/:path',
                text: 'Edit this page on GitHub'
            },
            lastUpdated: {
                text: "Last Updated"
            },
            nav: [
                { text: 'Getting Started', link: '/devdoc/README' },
                { text: 'hpccsystems.com', link: 'https://hpccsystems.com' },
                { text: 'Changelog', link: 'https://hpccsystems.com/download/release-notes/' },
            ],
            socialLinks: [
                { icon: 'github', link: 'https://github.com/hpcc-systems/HPCC-Platform' },
                { icon: 'linkedin', link: 'https://www.linkedin.com/company/hpcc-systems/home' },
                { icon: 'twitter', link: 'https://twitter.com/hpccsystems' },
                { icon: 'facebook', link: 'https://www.facebook.com/hpccsystems' },
                { icon: 'youtube', link: 'https://www.youtube.com/user/HPCCSystems' },
            ],
            sidebar: [
                {
                    text: 'General',
                    items: [
                        { text: 'Getting Started', link: '/devdoc/README' },
                        { text: 'Development Guide', link: '/devdoc/Development' },
                        { text: 'C++ Style Guide', link: '/devdoc/StyleGuide' },
                        { text: 'ECL Style Guide', link: '/ecllibrary/StyleGuide.html' },
                        { text: 'Code Submission Guidelines', link: '/devdoc/CodeSubmissions' },
                        { text: 'Code Review Guidelines', link: '/devdoc/CodeReviews' },
                        { text: 'Writing Developer Documentation', link: '/devdoc/DevDocs' },
                        { text: 'User Guides', link: '/devdoc/userdoc/README' },
                        { text: 'Build on Github Actions', link: '/devdoc/UserBuildAssets' },
                    ]
                },
                {
                    text: 'Other',
                    items: [
                        { text: 'Workunit Workflow', link: '/devdoc/Workunits' },
                        { text: 'Code Generator', link: '/devdoc/CodeGenerator' },
                        { text: 'Roxie', link: '/devdoc/roxie' },
                        { text: 'Memory Manager', link: '/devdoc/MemoryManager' },
                        { text: 'Metrics', link: '/devdoc/Metrics' },
                        { text: 'Development Testing With LDAP', link: '/devdoc/DevTestWithLDAP' },
                    ]
                }

            ],
            footer: {
                message: 'Released under the Apache-2.0 License.',
                copyright: 'Copyright © 2023-present hpccsystems.com'
            }
        },
        markdown: {
            // https://github.com/vuejs/vitepress/blob/main/src/node/markdown/markdown.ts
            config: md => {
                md.use(observable, { vitePress: true });
            },

            languages: [eclLang()],
        }
    });
};
