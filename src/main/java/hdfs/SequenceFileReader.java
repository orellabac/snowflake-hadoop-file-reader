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
    public String receiverId;
    public String parentReceiverId;
    public String receiverModel;
    public String equipmentModel;
    public String accountId;
    public String dma;
    public String postalCode;
    public String provisionedLocal;
    public String receivers;
    public String serviceCodes;
    public String unresolvedToken;
    public String accountType;
    public String unresolvedTokenPayloadConfirmationCode;
    public String receiverInstallDate;
    public String[] payloadArray;	
    public String inputPath;


    public DataRow(String key,String content) {
        String[] receiverPayloads = content.split("\001");
         receiverId = receiverPayloads[0];
         parentReceiverId = receiverPayloads[1];
         receiverModel = receiverPayloads[2];
         equipmentModel = receiverPayloads[3];
         accountId = replaceNull(receiverPayloads[4]);
         dma = replaceNull(receiverPayloads[5]);
         postalCode = replaceNull(receiverPayloads[6]);
         provisionedLocal = receiverPayloads[7];
         receivers = receiverPayloads[8];
         serviceCodes = receiverPayloads[9];
         unresolvedToken = replaceNull(receiverPayloads[10]);
         accountType = replaceNull(receiverPayloads[11]);
         unresolvedTokenPayloadConfirmationCode = replaceNull(receiverPayloads[12]);
         receiverInstallDate = replaceNull(receiverPayloads[14]);
         payloadArray = receiverPayloads[13].split("\002");	
         inputPath = receiverPayloads[14];
    }

    public static String replaceNull(String inStr) {
		if (inStr != null && inStr.equalsIgnoreCase("null"))
			return "";
		else
			return inStr;
	}

    public String toString() {
        var sb = new StringBuilder();
        try {
            for(var f :this.getClass().getFields())
            {
                sb.append(f.getName());
                sb.append(":");
            
                    sb.append(f.get(this));

                sb.append(",");
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return sb.toString();
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