
public class Main {
    public static void main(String[] args) {
        String pathStr = "./example.parquet";
        //ColumnWriter.testWrite();
        AvroWriter.testWrite(pathStr);
    }
}
