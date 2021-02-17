package br.com.cesar.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class ReadingReportService {

    private static final Path SOURCE = new File("service-reading-report/src/main/resources/report.txt").toPath();

    public static void main(String[] args) {
        var topic = "USER_GENERATE_READING_REPORT";
        var reportService = new ReadingReportService();
        try (var service = new KafkaService<>(ReadingReportService.class.getSimpleName(),
                topic,
                reportService::parse,
                User.class)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, User> record) throws IOException {
        System.out.println("Processing record for user.");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        var target = new File(record.value().getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " + record.value().getUuid());

        System.out.println("File created: " + target.getAbsolutePath());
    }
}
