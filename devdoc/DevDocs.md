# Working with developer documentation

_Some basic guidlines to ensure your documentation works well with VitePress_

## Documentation location

Documents can be located anywhere in the repository folder structure.  If it makes sense to have documentation "close" to specific components, then it can be located in the same folder as the component.  For example, any developer documentation for specific plugins can be located in those folders.  If this isn't appropriate then the documentation can be located in the `devdoc` or subfolders of `devdoc`.

:::warning
There is an exclusion list in the `devdoc/.vitepress/config.js` file that prevents certain folders from being included in the documentation.  If you add a new document to a folder that is excluded, then it will not be included in the documentation.  If you need to add a new document to an excluded folder, then you will need to update the exclusion list in the `devdoc/.vitepress/config.js` file.
:::

## Documentation format

Documentation is written in [Markdown](https://www.markdownguide.org/).  This is a simple format that is easy to read and write.  It is also easy to convert to other formats, such as HTML, PDF, and Word.  Markdown is supported by many editors, including Visual Studio Code, and is supported by VitePress.  

:::tip
VitePress extends Markdown with some additional features, such as [custom containers](https://vitepress.vuejs.org/guide/markdown.html#custom-containers), it is recommended that you refer to the [VitePress documentation](https://vitepress.dev/) for more details.
:::

## Rendering documentation locally with VitePress

_To assist with the writing of documentation, VitePress can be used to render the documentation locally.  This allows you to see how the documentation will look when it is published.  To start the local development server you need to type the following commands in the root HPCC-Platform folder:_

```sh
npm install
npm run docs-dev
```

This will start a local development server and display the URL that you can use to view the documentation.  The default URL is http://localhost:5173/HPCC-Platform, but it may be different on your machine.  The server will automatically reload when you make changes to the documentation.

:::warning
The first time you start the VitePress server it will take a while to complete.  This is because it is locating all the markdown files in the repository and creating the html pages.  Once it has completed this step once, it will be much faster to start the server again.
:::

## Adding a new document

To add a new document, you need to add a new markdown file to the repository.  The file should be named appropriately and have the `.md` file extension.  Once the file exists, you can view it by navigating to the appropriate URL.  For example, if you add a new file called `MyNewDocument.md` to the `devdoc` folder, then you can view it by navigating to http://localhost:5173/HPCC-Platform/devdoc/MyNewDocument.html.

## Adding a new document to the sidebar

To add a new document to the sidebar, you need to add an entry to the `devdoc/.vitepress/config.js` file.  The entry should be added to the `sidebar` section.  For example, to add a new document called `MyNewDocument.md` to the `devdoc` folder, you would add the following entry to the `sidebar` section:

```js
sidebar: [
    ...
    {
        text: 'My New Document',
        link: '/devdoc/MyNewDocument'
    }
    ...
```

:::tip
You can find more information on the config.js file in the [VitePress documentation](https://vitepress.dev/reference/site-config).
:::

## Editing the main landing page

The conent of the main landing page is located in `index.md` in the root folder.  Its structure uses the [VitePress "home" layout](https://vitepress.dev/reference/default-theme-home-page).