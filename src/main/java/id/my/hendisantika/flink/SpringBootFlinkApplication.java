package id.my.hendisantika.flink;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class SpringBootFlinkApplication {

    public static void main(String[] args) {
//        SpringApplication.run(SpringBootFlinkApplication.class, args);
        ConfigurableApplicationContext context = SpringApplication.run(SpringBootFlinkApplication.class, args);

        // 获取 FlinkJobService Bean，并运行 Flink 作业
        FlinkJobService flinkJobService = context.getBean(FlinkJobService.class);
        try {
            flinkJobService.runFlinkJob();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
