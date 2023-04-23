import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVReader;
import java.io.StringReader;

public class CleanMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 1st - tree diameter; drop status dead&stump
        // tree_id(1), tree_dbh(4), zipcode(26), zip_city(27), boroname(30)
        // 2nd - status
        // tree_id(1), status(7), zipcode(26), zip_city(27), boroname(30)
        // 3rd - health; drop status dead&stump
        // tree_id(1), health(8), zipcode(26), zip_city(27), boroname(30)
        
        try (CSVReader reader = new CSVReader(new StringReader(value.toString()));) {
            String[] n = reader.readNext();
            String status = n[7];
            // Drop rows with status == Dead or Stump
            if (status.equals("Alive")) {
                String tree_id = n[1];
                String tree_dbh = n[4];
                String health = n[8];
                String zipcode = n[26];
                String zip_city = n[27];
                String boroname = n[30];
                String key_word = String.join(",", tree_id, tree_dbh, health, zipcode, zip_city, boroname);
                context.write(new Text(key_word), new Text());
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
