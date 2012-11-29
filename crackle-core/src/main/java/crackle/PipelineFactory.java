package crackle;

import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;

public final class PipelineFactory {

  private PipelineFactory() { }

  public static Pipeline getPipeline(boolean inMemory) {
    return inMemory ? MemPipeline.getInstance() : new MRPipeline(PipelineFactory.class);
  }
}
