// https://vitepress.dev/guide/custom-theme
import { h } from "vue";
import DefaultTheme from "vitepress/theme";
import RenderComponent from "@hpcc-js/markdown-it-plugins/vitepress/RenderComponent.vue";
import "@hpcc-js/markdown-it-plugins/vitepress/styles.ts";

export default {
  extends: DefaultTheme,
  Layout: () => {
    return h(DefaultTheme.Layout, null, {
      // https://vitepress.dev/guide/extending-default-theme#layout-slots
    });
  },
  enhanceApp({ app }) {
    app.component("RenderComponent", RenderComponent);
  },

};
