import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.ByteArrayOutputStream;
import java.util.zip.ZipInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
/**
 * Extends the basic FileInputFormat to accept ZIP files.
 * ZIP files are not 'splittable', so we need to process/decompress in place:
 * each ZIP file will be processed by a single Mapper; we are parallelizing files, not lines...
 */
public class NYUZInputFormat extends FileInputFormat<Text, BytesWritable> {

    private List<Long> getOffsets(JobContext context, Path path) throws IOException {
        List<Long> offsets = new ArrayList<Long>();
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);
        ZipInputStream zis = new ZipInputStream(fis);

        long offsetValue = 0L;
        offsets.add(offsetValue);

        while (zis.getNextEntry() != null) {
            offsetValue += getByteSizeEntry(zis);
            offsets.add(offsetValue);
            zis.closeEntry();
        }

        fis.close();
        zis.close();
        return offsets;
    }

    private long getByteSizeEntry(ZipInputStream zis) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int len;
        while ((len = zis.read(buffer)) > 0) {
            bos.write(buffer, 0, len);
        }
        return (long)bos.size();
    }

    @Override
    public List<InputSplit> getSplits(JobContext context)
            throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        Path path = getInputPaths(context)[0];
        List<Long> offsets = getOffsets(context, path);
        long lastStartValue = 0;
        for(Long offLength: offsets){
            splits.add(new FileSplit(path, lastStartValue, offLength, null));
            lastStartValue = offLength;
        }
        return splits;
    }

    /*** return a record reader
     *
     * @param split
     * @param context
     * @return (Text, BytesWritable)
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        // no need to modify this one....
        return new NYUZRecordReader();
    }
}