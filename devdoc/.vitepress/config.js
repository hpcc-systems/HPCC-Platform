export default {
    title: 'HPCC Platform',
    description: 'The HPCC-Platform from hpccsystems is an open source system for big data analysis. It uses a single language, platform and architecture to process data efficiently and fast.',
    base: '/HPCC-Platform/',
    srcDir: '..',
    srcExclude: ["build/**", "dockerfiles/**", "docs/**", "esp/**", "helm/**", "initfiles/**", "plugins/**", "vcpkg/**" ],

    themeConfig: {
        repo: "hpcc-systems/HPCC-Platform",
        docsBranch: "master",
        editLink: {
            pattern: 'https://github.com/hpcc-systems/HPCC-Platform/edit/master/:path',
            text: 'Edit this page on GitHub'
        },
        lastUpdated: "Last Updated",
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
                    { text: 'Writing Developer Documentation', link: '/devdoc/DevDocs' },
                ]
            },
            {
                text: 'Other',
                items: [
                    { text: 'Workunit Workflow', link: '/devdoc/Workunits' },
                    { text: 'Code Generator', link: '/devdoc/CodeGenerator' },
                    { text: 'Memory Manager', link: '/devdoc/MemoryManager' },
                    { text: 'Metrics', link: '/devdoc/Metrics' },
                ]
            }

        ],
        footer: {
            message: 'Released under the Apache-2.0 License.',
            copyright: 'Copyright © 2023-present hpccsystems.com'
        }
    }
}
