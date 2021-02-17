package br.com.cesar.ecommerce;

public class User {

    private final String uuid;

    public User(String uuid) {
        this.uuid = uuid;
    }

    public String getReportPath() {
        return "service-reading-report/target/" + uuid + "_report.txt";
    }

    public String getUuid() {
        return uuid;
    }
}
