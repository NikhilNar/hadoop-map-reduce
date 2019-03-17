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

    @Override
    public List<InputSplit> getSplits(JobContext context)
            throws IOException {
        ArrayList<InputSplit> inputSplits = new ArrayList<InputSplit>();
        Path inputPath = getInputPaths(context)[0];
        List<Long> offsets = getOffsetsForSplits(context, inputPath);
        long prevStart = 0;
        for (int i = 1; i < offsets.size(); i++) {
            inputSplits.add(new FileSplit(inputPath, prevStart, offsets.get(i), null));
            prevStart = offsets.get(i);
        }
        return inputSplits;
    }

    /**
     * This function returns the offset at which each file resides.
     *
     * @param context   - the input context
     * @param inputPath - the path which contains the zip file.
     * @return -
     * @throws IOException -
     */
    private List<Long> getOffsetsForSplits(JobContext context, Path inputPath) throws IOException {
        List<Long> offsets = new ArrayList<Long>();
        FileSystem fileSystem = inputPath.getFileSystem(context.getConfiguration());
        FSDataInputStream stream = fileSystem.open(inputPath);
        ZipInputStream zipInputStream = new ZipInputStream(stream);

        //Add initial Offset
        offsets.add(0L);
        long previousOffset = 0L;
        ZipEntry entry;
        //As long as an entry is present, Iterate over it.
        while ((entry = zipInputStream.getNextEntry()) != null) {

            long currentOffset = previousOffset + (long) getSizeForThisEntry(zipInputStream);
            //If the filename of the entry starts with an alphanumeric character, only then add the offset.
            if (!entry.getName().startsWith("[^A-Za-z0-9]")) {
                offsets.add(currentOffset);
            }
            //Close this entry
            zipInputStream.closeEntry();
            previousOffset = currentOffset;
        }
        //Close streams
        zipInputStream.close();
        stream.close();
        return offsets;
    }

    /**
     * This function gets the length of an entry
     *
     * @param zipInputStream - the zip stream from which we need to check size of the entry
     * @return - size of the entry
     * @throws IOException
     */
    private int getSizeForThisEntry(ZipInputStream zipInputStream) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byte[] temp = new byte[8192];
        while (true) {
            //Keep reading as long as we can. Only then will we get the true size of the entry
            int bytesRead = 0;
            try {
                bytesRead = zipInputStream.read(temp, 0, 8192);
            } catch (EOFException e) {
                return 0;
            }
            if (bytesRead > 0){
                byteArrayOutputStream.write(temp, 0, bytesRead);
            }
            else
                break;
        }
        return byteArrayOutputStream.size();
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