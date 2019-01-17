package example.streaming;

import org.apache.beam.sdk.transforms.DoFn;

public class NoopFn<I> extends DoFn<I, I> {
    @ProcessElement
    public void process(ProcessContext processContext) {
        I element = processContext.element();
        processContext.output(element);
    }
}
