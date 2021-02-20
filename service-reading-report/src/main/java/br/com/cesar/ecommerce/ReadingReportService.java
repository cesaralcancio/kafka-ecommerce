package br.com.cesar.ecommerce;

import br.com.cesar.ecommerce.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class ReadingReportService {

    private static final Path SOURCE = new File("service-reading-report/src/main/resources/report.txt").toPath();

    public static void main(String[] args) {
        var topic = "ECOMMERCE_USER_GENERATE_READING_REPORT";
        var reportService = new ReadingReportService();
        try (var service = new KafkaService(ReadingReportService.class.getSimpleName(),
                topic,
                reportService::parse)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        var target = new File(record.value().getPayload().getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " + record.value().getPayload().getUuid());

        System.out.println("File created: " + target.getAbsolutePath());
    }
}
