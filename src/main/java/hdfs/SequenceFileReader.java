package hdfs;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import com.snowflake.snowpark_java.types.SnowflakeFile;
import java.io.FileOutputStream;

class DataRow {
    public String customer_no;
    public String account_no;
    public DataRow(String key, String content) {
        var parts = key.split(" ");
        customer_no = parts[0];
        account_no = parts[1];
    }
    public String toString() {
        return String.format("%s,%s", customer_no, account_no);
    }
}

public class SequenceFileReader {
    SequenceFile.Reader reader = null;
    FileSystem fs = null;
    Configuration conf;
    public SequenceFileReader() {
        try {
            this.conf = new Configuration();
            this.conf.set("fs.file.impl.disable.cache","true");
            this.fs = FileSystem.get(conf);
        } catch (IOException ex) { 
            throw new RuntimeException(ex);
        }
    }
    public static Class getOutputClass() { return DataRow.class; }

    private Path copyLocally(String path) 
    {
        try (
            InputStream inputStream = SnowflakeFile.newInstance(path, false).getInputStream();
            BufferedOutputStream bufferedOutputStream = 
                new BufferedOutputStream(new FileOutputStream("/tmp/inputfile.dat"))) 
            {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    bufferedOutputStream.write(buffer, 0, bytesRead);
                }
                return new Path("file:///tmp/inputfile.dat");
        } catch (IOException e) 
        { throw new RuntimeException(e); }        
    }
    public Stream<DataRow> process(String path)  {
        try  
        {
            this.reader = new SequenceFile.Reader(fs, copyLocally(path), conf);
            Writable key   = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            Stream<DataRow> resultStream = Stream.generate(() -> {
                try {
                    this.reader.next(key, value);//skip header
                    if (this.reader.next(key, value)) {
                            var keyText   = key.toString();
                            var valueText = value.toString();
                            return new DataRow(keyText, valueText);
                    }
                } catch (IOException e) {
                        e.printStackTrace();
                } 
                return null; 
            }).takeWhile(Objects::nonNull);
            return resultStream;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    

    public static void main(String[] args) throws FileNotFoundException {
        String uri = args[0];
        var reader = new SequenceFileReader();
        reader.process(uri).forEach(System.out::println);
    }

}