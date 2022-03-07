import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;



public class reports {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        PCollection<String> students = p.apply(TextIO.read().from("D:\\Desktop\\Raj Desktop\\beam java\\src\\main\\java\\reports data\\students.csv"));

        PCollection<String> output = students.apply(MapElements.via(new calculatePercentage()));

        output.apply(TextIO.write().to("D:\\Desktop\\Raj Desktop\\beam java\\src\\main\\java\\reports data\\reports").withNumShards(1).withSuffix(".csv"));

        p.run();

    }
}

class calculatePercentage extends SimpleFunction<String , String> {

    @Override
    public String apply(String input) {
        System.out.println(input);
        String op[] = input.split(",");
        String name = op[0];
        String roll = op[1];
        String marks = op[2];
        String total = op[3];

        if (name.trim().equals("Name")){
            return "";
        }
        float percentage = ((float)Integer.parseInt(marks)/Integer.parseInt(total))*100;

        return name + "," + roll + "," + Float.toString(percentage);
    }
}