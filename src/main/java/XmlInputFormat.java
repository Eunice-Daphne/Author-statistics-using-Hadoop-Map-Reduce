import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;

/**
 * An implementation of the Apache Mahout's XML Input Format to read more than one tag
 * https://github.com/apache/mahout/blob/66f164057e322d2e63ea02c35c9e30c3969e80b1/integration/src/main/java/org/apache/mahout/text/wikipedia/XmlInputFormat.java
 * Reads records that are delimited by a specific begin/end tag.
 */
public class XmlInputFormat extends TextInputFormat {

    public static final String START_TAG_KEYS = "xmlinput.start";
    public static final String END_TAG_KEYS = "xmlinput.end";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit is, TaskAttemptContext tac) {
        return new XmlRecordReader();
    }

    /**
     * XMLRecordReader class to read through a given xml document to output xml blocks as records as specified
     * by the start tag and end tag
     *
     */
    public static class XmlRecordReader extends RecordReader<LongWritable, Text> {
        private byte[][] startTags;
        private byte[][] endTags;
        private long start;
        private long end;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable key = new LongWritable();
        private Text value = new Text();

        private int x;

        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) is;

            //startTag = tac.getConfiguration().get(START_TAG_KEY).getBytes("utf-8");
            //endTag = tac.getConfiguration().get(END_TAG_KEY).getBytes("utf-8");

            /**
             * Store multiple start and end tags
             */
            String[] sTags = tac.getConfiguration().get(START_TAG_KEYS).split(",");
            String[] eTags = tac.getConfiguration().get(END_TAG_KEYS).split(",");

            startTags = new byte[sTags.length][];
            endTags = new byte[sTags.length][];

            for (int i = 0; i < sTags.length; i++) {
                startTags[i] = sTags[i].getBytes(StandardCharsets.UTF_8);
                endTags[i] = eTags[i].getBytes(StandardCharsets.UTF_8);
            }

            /**
             * open the file and seek to the start of the split
             */

            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Path file = fileSplit.getPath();

            FileSystem fs = file.getFileSystem(tac.getConfiguration());
            fsin = fs.open(fileSplit.getPath());
            fsin.seek(start);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (fsin.getPos() < end) {
                if (readUntilMatch(startTags, false)) {
                    try {
                        buffer.write(startTags[x - 1]);
                        if (readUntilMatch(endTags, true)) {
                            value.set(buffer.getData(), 0, buffer.getLength());
                            key.set(fsin.getPos());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (fsin.getPos() - start) / (float) (end - start);
        }

        @Override
        public void close() throws IOException {
            fsin.close();
        }

        private boolean readUntilMatch(byte[][] match, boolean withinBlock) throws IOException {

            int[] tags = new int[match.length];

            while (true) {
                int b = fsin.read();
                /**
                 *  end of file
                  */

                if (b == -1) {
                    return false;
                }
                /**
                 * save to buffer
                 */

                if (withinBlock) {
                    buffer.write(b);
                }

                /*
                if (b == match[i]) {
                    i++;
                   if (i >= match.length) {
                        return true;
                      }
                    } else {
                      i = 0;
                    }
                     */
                /**
                 check if we're matching:
                 */
                for (int i = 0; i < tags.length; i++) {
                    if (b == match[i][tags[i]]) {
                        tags[i]++;
                        if (tags[i] >= match[i].length) {
                            x = i + 1;
                            return true;
                        }
                    }
                    else
                        tags[i] = 0;
                }
                /**
                 see if we've passed the stop point
                 */
                boolean status = (IntStream.of(tags).sum()==0) ? true : false;
                if (!withinBlock && status && fsin.getPos() >= end)
                    return false;
            }
        }
    }
}
