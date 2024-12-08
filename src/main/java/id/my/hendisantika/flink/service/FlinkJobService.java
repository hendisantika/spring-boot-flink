package id.my.hendisantika.flink.service;

import id.my.hendisantika.flink.job.StringProcessingJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by IntelliJ IDEA.
 * Project : spring-boot-flink
 * User: hendisantika
 * Email: hendisantika@gmail.com
 * Telegram : @hendisantika34
 * Date: 08/12/24
 * Time: 08.01
 * To change this template use File | Settings | File Templates.
 */
@Service
//@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public class FlinkJobService {

    //    private final StringProcessingJob stringProcessingJob;
    @Autowired
    private StringProcessingJob stringProcessingJob;

    public void runFlinkJob() throws Exception {
        StringProcessingJob.main(new String[]{});
    }
}
