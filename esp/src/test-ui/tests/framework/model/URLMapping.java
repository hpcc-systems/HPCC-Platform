package framework.model;

import java.util.HashMap;

public class URLMapping {

    String name;
    String url;
    HashMap<String, URLMapping> urlMappings;

    public URLMapping(String name, String url) {
        this.name = name;
        this.url = url;
        urlMappings = new HashMap<>();
    }

    public String getUrl() {
        return url;
    }

    public HashMap<String, URLMapping> getUrlMappings() {
        return urlMappings;
    }

    @Override
    public String toString() {
        return "URLMapping{" +
                "name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", urlMappings=" + urlMappings +
                '}';
    }
}
