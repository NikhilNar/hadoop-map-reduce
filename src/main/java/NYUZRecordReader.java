import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipEntry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;


/*** Custom Hadoop Record Reader : zipped file
 *
 * We want to produce (K,V) pairs where
 *    K = filename inside the zip file
 *    V = bytes corresponding to the file
 *
 * ***/
public class NYUZRecordReader extends RecordReader<Text, BytesWritable> {

   private FSDataInputStream fis;
   private ZipInputStream zis;
   private Text currentKey;
   private BytesWritable currentValue;
   private boolean isfinished;
   private Long bytesSkipped;

   private ByteArrayOutputStream getByteStream(ZipInputStream zis) throws IOException {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      byte[] buffer = new byte[4096];
      int len;
      while ((len = zis.read(buffer)) > 0) {
         bos.write(buffer, 0, len);
      }
      return bos;
   }

   @Override
   public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
      FileSplit split = (FileSplit) inputSplit;
      Path path = split.getPath();
      FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
      fis = fileSystem.open(path);
      zis = new ZipInputStream(fis);
      bytesSkipped = ((FileSplit) inputSplit).getStart();
   }

   @Override
   public boolean nextKeyValue() throws IOException, InterruptedException {
      if (isfinished) return false;

      findSplitStart();

      ZipEntry entry = zis.getNextEntry();
      if (entry == null)
         return false;

      currentKey = new Text(entry.getName());
      currentValue = new BytesWritable(getByteStream(zis).toByteArray());

      isfinished = true;
      zis.closeEntry();
      return true;
   }

   private void findSplitStart() throws IOException {
      while (bytesSkipped > 0) {
         zis.getNextEntry();
         bytesSkipped -= zis.skip(bytesSkipped);
         zis.closeEntry();
      }
   }

   @Override
   public Text getCurrentKey() throws IOException, InterruptedException {
      return currentKey;
   }

   @Override
   public BytesWritable getCurrentValue() throws IOException, InterruptedException {
      return currentValue;
   }

   @Override
   public float getProgress() throws IOException, InterruptedException {
      return isfinished ? 1.0f : 0.0f;
   }

   @Override
   public void close() throws IOException {
      try {
         zis.close();
         fis.close();
      } catch (Exception e) {
         System.out.println("Not able to close the streams");
      }
   }
}