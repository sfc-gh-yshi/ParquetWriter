import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.*;


public class AvroWriter {
    public static final String schema = "{\"namespace\": \"example.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"map\",\n" +
            " \"fields\": [\n" +
            "   {\"name\": \"col1\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}\n" +
            "  ]\n" +
            "}";

    public static final Schema avroSchema = new Schema.Parser().parse(schema);

    public static void testWrite(String pathStr){

        MessageType parquetSchema = new AvroSchemaConverter().convert(avroSchema);
        System.out.println(parquetSchema.toString());
        Map<String, String> map = new HashMap<>();
        map.put("test", "something");
        GenericRecord record = new GenericRecordBuilder(avroSchema).set("col1", map).build();


        Path filePath = new Path("./example.parquet");
        try {
            filePath.getFileSystem(new Configuration()).delete(filePath, true);
        }catch (IOException e) {
            e.printStackTrace();
        }
        int blockSize = 1024;
        int pageSize = 65535;
        try(
                AvroParquetWriter parquetWriter = new AvroParquetWriter(
                        filePath,
                        avroSchema,
                        CompressionCodecName.UNCOMPRESSED,
                        blockSize,
                        pageSize)
        ){
            //for(String obj : dataToWrite){
                parquetWriter.write(record);
            //}
        }catch(java.io.IOException e){
            System.out.println(String.format("Error writing parquet file %s", e.getMessage()));
            e.printStackTrace();
        }
    }
}
