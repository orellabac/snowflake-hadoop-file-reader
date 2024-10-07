package hdfs;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import java.io.FileOutputStream;
import java.util.Base64;
import java.util.Base64.Encoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;


class GenericDataRow {
    public String key;
    public String content;


    public GenericDataRow(String key,String content) {
         this.key = key;
         this.content = content;
    }

    public static String replaceNull(String inStr) {
		if (inStr != null && inStr.equalsIgnoreCase("null"))
			return "";
		else
			return inStr;
	}

}

public class GenericSequenceFileReader {
    SequenceFile.Reader reader = null;
    FileSystem fs = null;
    Configuration conf;
    public static Encoder encoder = Base64.getEncoder();

    public GenericSequenceFileReader() {
        try {
            this.conf = new Configuration();
            this.conf.set("fs.file.impl.disable.cache","true");
            this.fs = FileSystem.get(conf);
        } catch (IOException ex) { 
            throw new RuntimeException(ex);
        }
    }
    public static Class getOutputClass() { return GenericDataRow.class; }

    private Path copyLocally(String path) 
    {
        try (
            InputStream inputStream = Files.newInputStream(java.nio.file.Path.of(path));
            //SnowflakeFile.newInstance(path, false).getInputStream();
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

    public static byte[] decompressGzip(byte[] compressedData) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(compressedData);
        GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        
        byte[] buffer = new byte[1024];
        int len;
        
        while ((len = gzipInputStream.read(buffer)) != -1) {
            byteArrayOutputStream.write(buffer, 0, len);
        }
        
        gzipInputStream.close();
        byteArrayOutputStream.close();
        
        return byteArrayOutputStream.toByteArray();
    }

    public Stream<GenericDataRow> process(String path)  {
        try  
        {
            this.reader = new SequenceFile.Reader(fs, copyLocally(path), conf);
            Writable key   = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            Class<?> valueClass = reader.getValueClass();
            final boolean isBytes = BytesWritable.class.isAssignableFrom(valueClass);
            
            Stream<GenericDataRow> resultStream = Stream.generate(() -> {
                try {
                    this.reader.next(key, value);//skip header
                    if (this.reader.next(key, value)) {
                            var keyText   = key.toString();
                            String valueText = null;
                            if (isBytes) {
                                // Cast to BytesWritable
                                BytesWritable bytesWritable = (BytesWritable) value;

                                // Now you can use bytesWritable as needed
                                byte[] byteArray = bytesWritable.getBytes();
                                if (byteArray != null && byteArray.length > 4 && 
                                byteArray[0] == (byte) (GZIPInputStream.GZIP_MAGIC) && 
                                byteArray[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8) && 
                                byteArray[2] == 0x08 && 
                                byteArray[3] == 0x00)
                                {
                                    // GZIP DEFLATE
                                    byte[] decompressed = decompressGzip(byteArray);
                                    try {
                                        valueText = new String(decompressed);
                                    }
                                    catch (Exception e) {
                                        valueText = encoder.encodeToString(byteArray);
                                    }
                                }
                                else {
                                    // just encode the bytes in base64
                                    valueText = encoder.encodeToString(byteArray);
                                }
                            }
                            else {
                                valueText = value.toString();
                            }
                            
                            return new GenericDataRow(keyText, valueText);
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
        String uri = "/Users/mrojas/Downloads/payload.20241004.084401.1.seq";
        var reader = new GenericSequenceFileReader();
        reader.process(uri).forEach(System.out::println);
    }

}
