
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import static org.apache.parquet.example.Paper.r1;
import static org.apache.parquet.example.Paper.r2;
import static org.apache.parquet.example.Paper.schema;
import static org.apache.parquet.example.Paper.schema2;
import static org.apache.parquet.example.Paper.schema3;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnWriteStoreV2;
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.page.*;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class ColumnWriter {
    private static final String schema =
            "message myrecord {\n" +
                    "  optional group col1 (MAP_KEY_VALUE) {\n" +
                    "    repeated group map {\n" +
                    "      required binary key (String);\n" +
                    "      optional binary value (String);\n" +
                    "    }\n" +
                    "  }\n" +
                    "}\n";

    private static final MessageType convertedSchema = MessageTypeParser.parseMessageType(schema);

    public static void testWrite() {
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(convertedSchema);
        PageWriteStore pageWriteStore = columnDescriptor -> null;
        ColumnWriteStoreV1 columns = new ColumnWriteStoreV1(convertedSchema,
                pageWriteStore,
                ParquetProperties.builder()
                .withPageSize(50*1024*1024)
                .withDictionaryEncoding(false)
                .build());
        RecordConsumer recordWriter = columnIO.getRecordWriter(columns);
        GroupWriter groupWriter = new GroupWriter(recordWriter, convertedSchema);
        groupWriter.write(r1);
        groupWriter.write(r2);
        recordWriter.flush();
        columns.flush();
        columns.close();
    }


}
