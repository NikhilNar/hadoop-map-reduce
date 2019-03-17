import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
   private FSDataInputStream fsDataInputStream;
   private ZipInputStream zipInputStream;
   public static final Log log = LogFactory.getLog(NYUZRecordReader.class);
   private Long amountToSkip;
   private boolean done;

   @Override
   public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit) inputSplit;
      Path zipPath = fileSplit.getPath();
      FileSystem fileSystem = zipPath.getFileSystem(context.getConfiguration());
      fsDataInputStream = fileSystem.open(zipPath);
      zipInputStream = new ZipInputStream(fsDataInputStream);
      amountToSkip = ((FileSplit) inputSplit).getStart();
   }

   @Override
   public boolean nextKeyValue() throws IOException, InterruptedException {
      if (done) {
         return false;
      }
      skipAmount();
      ZipEntry entry = zipInputStream.getNextEntry();
      if (entry == null) {
         return false;
      }
      currentKey = new Text(entry.getName());
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      byte[] bytes = new byte[4096];
      while (true) {
         int bytesRead = 0;
         try {
            bytesRead = zipInputStream.read(bytes, 0, 4096);
         } catch (EOFException e) {
            return false;
         }
         if (bytesRead > 0)
            byteArrayOutputStream.write(bytes, 0, bytesRead);
         else
            break;
      }
      zipInputStream.closeEntry();

      // Uncompressed contents
      currentValue = new BytesWritable(byteArrayOutputStream.toByteArray());
      //Make sure only 1 entry is processed
      done = true;
      return true;
   }

   /**
    * Function to skip to the desired entry.
    * Each split has a start offset, keep skipping till we reach there.
    * @throws IOException -
    */
   private void skipAmount() throws IOException {
      while (amountToSkip > 0) {
         zipInputStream.getNextEntry();
         long skipped = zipInputStream.skip(amountToSkip);
         amountToSkip -= skipped;
         zipInputStream.closeEntry();
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
      return done ? 1.0f : 0.0f;
   }

   @Override
   public void close() throws IOException {
      try {
         zipInputStream.close();
         fsDataInputStream.close();
      } catch (Exception e) {
         System.out.println("Could not close one of the streams");
      }
   }
}