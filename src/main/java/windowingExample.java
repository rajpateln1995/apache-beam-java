
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.util.Arrays;
import java.util.List;

public class windowingExample {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        PCollection<String> dailyData = p.apply(TextIO.read().from("D:\\Desktop\\Raj Desktop\\beam java\\src\\main\\java\\windowing\\NIFTY 50_Data.csv"));

        PCollection<List<String>> keyValue = dailyData.apply(MapElements.via(new KVPairs()));

        PCollection<List<String>> FixedWindow = keyValue.apply(Window.<List<String>>into(FixedWindows.of(Duration.standardDays(365))));

//        Trying to count elements in each window here
        PCollection<Long> count = FixedWindow.apply(Count.globally());

        PCollection<Long> output = count.apply(MapElements.via(new print()));



        p.run();

    }
}

class KVPairs extends SimpleFunction<String , List<String>> {

    @Override
    public List<String> apply(String input) {

        String data[] = input.split(",");

        List li = Arrays.asList(input , data[0]);
        return li;
    }
}

class print extends SimpleFunction<Long , Long> {

    @Override
    public Long apply(Long input) {
        System.out.println(input);

        return input;
    }
}





