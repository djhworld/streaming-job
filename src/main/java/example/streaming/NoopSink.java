package example.streaming;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class NoopSink<T> extends PTransform<PCollection<T>, PDone> {

    @Override
    public PDone expand(PCollection<T> input) {
        input.apply(ParDo.of(new WriteFn<>()));
        return PDone.in(input.getPipeline());
    }

    private static class WriteFn<T> extends DoFn<T, Void> {

        @StartBundle
        public void startBundle() throws Exception {
            // noop
        }

        @ProcessElement
        public void processElement(ProcessContext context) throws Exception {
            // noop
        }

        @FinishBundle
        public void finishBundle() throws Exception {
            // noop
        }

        @DoFn.Teardown
        public void teardown() throws Exception {
            // noop
        }
    }
}
