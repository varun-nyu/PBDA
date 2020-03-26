

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;



public class NYUSampleInputFormat extends TextInputFormat {

    private static final double SPLIT_SLOP = 1.1;   // 10% slop


    @Override


    public List<InputSplit> getSplits(JobContext job) throws IOException {

        // This is the original code for FileInputFormat
        // Directly used for hadoop.io source available on Github
        long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
        long maxSize = getMaxSplitSize(job);

        // generate splits
        List<InputSplit> splits = new ArrayList<InputSplit>();
        List<FileStatus> files = listStatus(job);
        for (FileStatus file: files) {
            Path path = file.getPath();
            long length = file.getLen();
            if (length != 0) {
                BlockLocation[] blkLocations;
                if (file instanceof LocatedFileStatus) {
                    blkLocations = ((LocatedFileStatus) file).getBlockLocations();
                } else {
                    FileSystem fs = path.getFileSystem(job.getConfiguration());
                    blkLocations = fs.getFileBlockLocations(file, 0, length);
                }
                if (isSplitable(job, path)) {
                    long blockSize = file.getBlockSize();
                    long splitSize = computeSplitSize(blockSize, minSize, maxSize);

                    long bytesRemaining = length;
                    while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
                        int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
                        splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                                blkLocations[blkIndex].getHosts()));
                        bytesRemaining -= splitSize;
                    }

                    if (bytesRemaining != 0) {
                        int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
                        splits.add(makeSplit(path, length-bytesRemaining, bytesRemaining,
                                blkLocations[blkIndex].getHosts()));
                    }
                } else { // not splitable
                    splits.add(makeSplit(path, 0, length, blkLocations[0].getHosts()));
                }
            } else {
                //Create empty hosts array for zero length files
                splits.add(makeSplit(path, 0, length, new String[0]));
            }
        }

        job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());

        /*
          Dividing K by number of splits to evenly and randomly select the lines
         */
        int randomSamplePerc = Integer.parseInt(job.getConfiguration().get("K"))/splits.size();
        job.getConfiguration().set("K",Integer.toString(randomSamplePerc));


        //System.out.println("total splits =>"+splits.size());
        return splits;
    }

    @Override
    protected boolean isSplitable(org.apache.hadoop.mapreduce.JobContext ctx, Path filename){
        return true;
    }


    /*** return a record reader
     *
     * @param split
     * @param context
     * @return (Text,BytesWritable)
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new NYUSampleRecordReader();
    }
}
