package id.my.hendisantika.flink;

import id.my.hendisantika.flink.service.FlinkJobService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class SpringBootFlinkApplication {

    public static void main(String[] args) {
//        SpringApplication.run(SpringBootFlinkApplication.class, args);
        ConfigurableApplicationContext context = SpringApplication.run(SpringBootFlinkApplication.class, args);

        // Get the FlinkJobService Bean and run the Flink job
        FlinkJobService flinkJobService = context.getBean(FlinkJobService.class);
        try {
            flinkJobService.runFlinkJob();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
