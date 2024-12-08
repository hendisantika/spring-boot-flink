package id.my.hendisantika.flink.job;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by IntelliJ IDEA.
 * Project : spring-boot-flink
 * User: hendisantika
 * Email: hendisantika@gmail.com
 * Telegram : @hendisantika34
 * Date: 08/12/24
 * Time: 07.56
 * To change this template use File | Settings | File Templates.
 */
public class FlinkWebUI {
    public static void main(String[] args) throws Exception {
        // Local Mode
        Configuration conf = new Configuration();
        // Specifying Ports
        conf.setString(RestOptions.BIND_PORT, "7777");
        //  Creating an execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        // Globally specify the degree of parallelism, the default is the number of threads on the computer
        env.setParallelism(2);

        // Read the socket text stream
        DataStreamSource<String> socketDS = env.socketTextStream("172.24.4.193", 8888);

        //  Process data: cut, transform, group, aggregate and obtain statistical results
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                )
                //Locally set operator parallelism
                .setParallelism(3)
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                // Locally set operator parallelism
                .setParallelism(4);

        //  Output
        sum.print();

        //  implement
        env.execute();
    }
}
