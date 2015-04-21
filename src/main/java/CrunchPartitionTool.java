import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.parquet.AvroParquetFileSource;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.CompressionType;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.View;
import org.kitesdk.data.crunch.CrunchDatasets;
import org.kitesdk.data.spi.PartitionStrategyParser;
import org.kitesdk.data.spi.Schemas;

public class CrunchPartitionTool extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    String partitionStrategyName = args[0];
    String inputPath = args[1];
    String outputPath = args[2];

    Configuration conf = getConf();

    Schema schema = readSchema(new Path(inputPath));

    Pipeline pipeline = new MRPipeline(CrunchPartitionTool.class, conf);
    PCollection<GenericData.Record> records = pipeline.read(
        new AvroParquetFileSource(new Path(inputPath), Avros.generics(schema)));

    DatasetDescriptor desc = new DatasetDescriptor.Builder()
        .schema(schema)
        .partitionStrategy(readPartitionStrategy(partitionStrategyName))
        .format(Formats.PARQUET)
        .compressionType(CompressionType.Uncompressed)
        .build();

    View<GenericData.Record> dataset = Datasets.create("dataset:" + outputPath, desc,
        GenericData.Record.class);

    int numReducers = conf.getInt("mapreduce.job.reduces", 1);
    System.out.println("Num reducers: " + numReducers);
    PCollection<GenericData.Record> partition =
        CrunchDatasets.partition(records, dataset, numReducers);

    pipeline.write(partition, CrunchDatasets.asTarget(dataset));

    PipelineResult result = pipeline.done();
    return result.succeeded() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new CrunchPartitionTool(), args);
    System.exit(exitCode);
  }

  private Schema readSchema(Path path) throws IOException {
    Path file;
    FileSystem fs = path.getFileSystem(getConf());
    if (fs.isDirectory(path)) {
      FileStatus[] fileStatuses = fs.listStatus(path, new PathFilter() {
        @Override
        public boolean accept(Path p) {
          String name = p.getName();
          return !name.startsWith("_") && !name.startsWith(".");
        }
      });
      file = fileStatuses[0].getPath();
    } else {
      file = path;
    }
    return Schemas.fromParquet(fs, file);
  }

  private PartitionStrategy readPartitionStrategy(String name) throws IOException {
    InputStream in = CrunchPartitionTool.class.getResourceAsStream(name + ".json");
    try {
      return PartitionStrategyParser.parse(in);
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }
}
