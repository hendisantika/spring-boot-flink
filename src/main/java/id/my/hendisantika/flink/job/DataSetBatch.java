package id.my.hendisantika.flink.job;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Created by IntelliJ IDEA.
 * Project : spring-boot-flink
 * User: hendisantika
 * Email: hendisantika@gmail.com
 * Telegram : @hendisantika34
 * Date: 08/12/24
 * Time: 07.52
 * To change this template use File | Settings | File Templates.
 */
public class DataSetBatch {
    public static void main(String[] args) throws Exception {
        // Creating an execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Get the file path
        String path = DataSetBatch.class.getClassLoader().getResource("word.txt").getPath();
        // Reading data from a file
        DataSource<String> lineDS = env.readTextFile(path);

        // Segmentation and conversion, for example: (word, 1)
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDS.flatMap(new MyFlatMapper());

        // Group by word Group by the first word in the word
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroupBy = wordAndOne.groupBy(0);

        // Aggregate statistics within a group Sum the data in the second position
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroupBy.sum(1);

        // Output
        sum.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        //value is input, out is output data
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // Split words by spaces
            String[] words = value.split(" ");
            // Traverse all words, pack them into two-tuple outputs and convert words into (word, 1)
            for (String word : words) {
                Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                //  Use Collector to send data downstream
                out.collect(wordTuple2);
            }
        }
    }
}
