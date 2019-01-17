package example.streaming;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticTestUtils;
import org.apache.beam.sdk.io.synthetic.SyntheticUnboundedSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        FlinkPipelineOptions jobOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(FlinkPipelineOptions.class);

        Read.Unbounded<KV<byte[], byte[]>> source = createUnboundedSource(jobOptions);
        NoopFn<KV<byte[], byte[]>> transformation = new NoopFn<>();
        NoopSink<KV<byte[], byte[]>> sink = new NoopSink<>();

        Pipeline pipeline = Pipeline.create(jobOptions);

        pipeline
                .apply(source)
                .apply(ParDo.of(transformation))
                .apply(sink);

        waitUntilFinished(pipeline.run());
    }

    private static Read.Unbounded<KV<byte[], byte[]>> createUnboundedSource(FlinkPipelineOptions jobOptions) throws IOException {
        String optionsJson =
                "{\"numRecords\":9223372036854775806,\"splitPointFrequencyRecords\":10,\"keySizeBytes\":10,"
                        + "\"valueSizeBytes\":20,\"numHotKeys\":3,"
                        + "\"hotKeyFraction\":0.3,\"seed\":123456,"
                        + "\"bundleSizeDistribution\":{\"type\":\"const\",\"const\":42},"
                        + "\"forceNumInitialBundles\":" + jobOptions.getParallelism() + "}";

        SyntheticSourceOptions syntheticSourceOptions = SyntheticTestUtils.optionsFromString(optionsJson, SyntheticSourceOptions.class);
        return Read.from(new SyntheticUnboundedSource(syntheticSourceOptions));
    }

    private static PipelineResult waitUntilFinished(PipelineResult result) {
        try {
            result.waitUntilFinish();
            return result;
        } catch (Exception exc) {
            exc.printStackTrace();
        }
        return result;
    }
}
