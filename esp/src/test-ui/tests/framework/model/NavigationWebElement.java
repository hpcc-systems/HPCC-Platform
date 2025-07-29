package framework.model;

import org.openqa.selenium.WebElement;

public record NavigationWebElement(String name, String hrefValue, WebElement webElement) {

    @Override
    public String toString() {
        return "NavigationWebElement{" +
                "name='" + name + '\'' +
                ", hrefValue='" + hrefValue + '\'' +
                '}';
    }
}
