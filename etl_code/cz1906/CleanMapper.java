import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import com.opencsv.exceptions.CsvMalformedLineException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class CleanMapper extends Mapper<Object, Text, Text, Text> {
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        CSVReader reader = new CSVReader(new StringReader(value.toString()));

        try {
            String[] n = reader.readNext();

            if (n != null) {
                String household_type = n[1];
                String time_frame = n[2];

                if (household_type.equals("All Households") && time_frame.equals("2015")) {
                    String location = n[0];
                    String data = n[4];
                    String key_word = String.join(",", location, data);
                    context.write(new Text(), new Text(key_word));
                }
            }
        } catch (CsvValidationException | CsvMalformedLineException e) {
            String errorMessage = e.getMessage();
            System.out.println(errorMessage);
        }
    }

}
