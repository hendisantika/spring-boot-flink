package id.my.hendisantika.flink.job;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Component;

/**
 * Created by IntelliJ IDEA.
 * Project : spring-boot-flink
 * User: hendisantika
 * Email: hendisantika@gmail.com
 * Telegram : @hendisantika34
 * Date: 08/12/24
 * Time: 07.59
 * To change this template use File | Settings | File Templates.
 */
@Component
public class StringProcessingJob {
    public static void main(String[] args) throws Exception {
        // Initialize the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Add a data source
        DataStream<String> text = env.fromElements("Hello", "Flink", "Spring Boot");

        // Data processing
        DataStream<String> processedText = text
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return "Processed: " + value;
                    }
                });

        // Output
        processedText.print();

        // Execute the job
        env.execute("String Processing Job");

    }
}
