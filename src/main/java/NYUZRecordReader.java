import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/*** Custom Hadoop Record Reader : zipped file
 *
 * We want to produce (K,V) pairs where
 *    K = filename inside the zip file
 *    V = bytes corresponding to the file
 *
 * ***/
public class NYUZRecordReader extends RecordReader<Text, BytesWritable> {

   private Text currentKey;
   private BytesWritable currentValue;
   private FSDataInputStream fis;
   private ZipInputStream zis;
   private boolean done;

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
   }

   @Override
   public boolean nextKeyValue() throws IOException, InterruptedException {
      if (done) {
         return false;
      }

      ZipEntry entry = zis.getNextEntry();
      if (entry == null) {
         done = true;
         return false;
      }
      currentKey = new Text(entry.getName());
      // Uncompressed contents
      currentValue = new BytesWritable(getByteStream(zis).toByteArray());
      //Make sure only 1 entry is processed

      return true;
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
      return done ? 1.0f : 0.0f;
   }

   @Override
   public void close() throws IOException {
      try {
         zis.close();
         fis.close();
      } catch (Exception e) {
         System.out.println("Not able to close streams");
      }
   }
}