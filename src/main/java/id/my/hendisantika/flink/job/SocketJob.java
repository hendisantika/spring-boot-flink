package id.my.hendisantika.flink.job;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * Time: 07.58
 * To change this template use File | Settings | File Templates.
 */
public class SocketJob {
    public static void main(String[] args) throws Exception {
        // Creating an execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Specify the degree of parallelism, the default number of computer threads
        env.setParallelism(3);
        // Read the data socket text stream, specify the listening IP port, and execute the task only when data is received.
        DataStreamSource<String> socketDS = env.socketTextStream("172.24.4.193", 8888);

        // Process data: switch, transform, group, aggregate and obtain statistical results
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                )
                .setParallelism(2)
                // Explicitly provide type information: For Lambda expressions passed to flatMap,
                // the system can only infer that the returned type is Tuple2, but cannot get Tuple2<String, Long>.
                // Only by explicitly setting the current return type of the system can the complete data be correctly parsed.
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                })
//                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);

        // Output
        sum.print();

        // implement
        env.execute();
    }
}
