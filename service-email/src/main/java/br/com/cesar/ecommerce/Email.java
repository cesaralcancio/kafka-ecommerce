package br.com.cesar.ecommerce;

public class Email {

    private final String subject, body;

    public Email(String subject, String body) {
        this.subject = subject;
        this.body = body;
    }

    @Override
    public String toString() {
        return "Email{" +
                "subject='" + subject + '\'' +
                ", body='" + body + '\'' +
                '}';
    }
}
