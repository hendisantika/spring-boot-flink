package id.my.hendisantika.flink.job;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
 * Time: 07.53
 * To change this template use File | Settings | File Templates.
 */
public class DataStreamAPI {
    public static void main(String[] args) throws Exception {
        // Creating an execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read data from the word.txt file in the data directory under the project root directory
        DataStreamSource<String> source = env.readTextFile("D:\\IdeaProjects\\spring-boot-flink\\src\\main\\resources\\word.txt");

        // Processing data: segmentation, conversion
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // Split by space
                        String[] words = value.split(" ");
                        for (String word : words) {
                            // Convert to bigram (word, 1)
                            Tuple2<String, Integer> wordsAndOne = Tuple2.of(word, 1);
                            // Send data downstream through the collector
                            out.collect(wordsAndOne);
                        }
                    }
                });

        // Processing data: Grouping
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS.keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }
        );
        // Processing data: Aggregation
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = wordAndOneKS.sum(1);

        // Output Data
        sumDS.print();

        // implement
        env.execute();
    }
}
