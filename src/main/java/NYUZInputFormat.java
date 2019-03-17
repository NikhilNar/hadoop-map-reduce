import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Extends the basic FileInputFormat to accept ZIP files.
 * ZIP files are not 'splittable', so we need to process/decompress in place:
 * each ZIP file will be processed by a single Mapper; we are parallelizing files, not lines...
 */
public class NYUZInputFormat extends FileInputFormat<Text, BytesWritable> {
    public static final Log log = LogFactory.getLog(NYUZInputFormat.class);

    /**
     * This function returns the offset at which each file resides.
     *
     * @param context   - the input context
     * @param inputPath - the path which contains the zip file.
     * @return -
     * @throws IOException -
     */
    private List<Long> getOffsets(JobContext context, Path inputPath) throws IOException {
        List<Long> offsets = new ArrayList<Long>();
        FileSystem fileSystem = inputPath.getFileSystem(context.getConfiguration());
        FSDataInputStream stream = fileSystem.open(inputPath);
        ZipInputStream zipInputStream = new ZipInputStream(stream);

        //Add initial Offset
        long offsetValue = 0L;
        offsets.add(offsetValue);
        ZipEntry entry;
        //As long as an entry is present, Iterate over it.
        while ((entry = zipInputStream.getNextEntry()) != null) {

            offsetValue +=  (long) getByteSizeEntry(zipInputStream);
            //If the filename of the entry starts with an alphanumeric character, only then add the offset.
            if (!entry.getName().startsWith("[^A-Za-z0-9]")) {
                offsets.add(offsetValue);
            }
            //Close this entry
            zipInputStream.closeEntry();
        }
        //Close streams
        zipInputStream.close();
        stream.close();
        return offsets;
    }

    /**
     * This function gets the length of an entry
     *
     * @param zis - the zip stream from which we need to check size of the entry
     * @return - size of the entry
     * @throws IOException
     */
    private int getByteSizeEntry(ZipInputStream zis) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int len;
        while ((len = zis.read(buffer)) > 0) {
            bos.write(buffer, 0, len);
        }
        return bos.size();
    }

    @Override
    public List<InputSplit> getSplits(JobContext context)
            throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        Path path = getInputPaths(context)[0];
        List<Long> offsets = getOffsets(context, path);
        long prevStart = 0;
        for (int i = 1; i < offsets.size(); i++) {
            splits.add(new FileSplit(path, prevStart, offsets.get(i), null));
            prevStart = offsets.get(i);
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