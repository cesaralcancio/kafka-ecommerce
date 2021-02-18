package br.com.cesar.ecommerce;

import br.com.cesar.ecommerce.dispatcher.KafkaDispatcher;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class GenerateAllReportsServlet extends HttpServlet {

    private final KafkaDispatcher<String> batchDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        this.batchDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        var topic = "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS";
        var msg = "ECOMMERCE_USER_GENERATE_READING_REPORT";
        try {
            batchDispatcher.sendAndWait(topic, new CorrelationId(GenerateAllReportsServlet.class.getName()), msg, msg);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.getWriter().println("Reports requested...");
    }
}
