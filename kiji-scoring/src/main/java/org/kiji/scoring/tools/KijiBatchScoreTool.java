package org.kiji.scoring.tools;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;

import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.mapreduce.output.HFileMapReduceJobOutput;
import org.kiji.mapreduce.output.KijiTableMapReduceJobOutput;
import org.kiji.mapreduce.tools.framework.KijiJobTool;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.tools.KijiToolLauncher;
import org.kiji.scoring.ScoreFunction;
import org.kiji.scoring.batch.ScoreFunctionJobBuilder;

/**
 * Command line tool for running ScoreFunctions in batch.
 *
 * <p>
 *  ScoreFunction MapReduce jobs run from the command line may not override the default client data
 *  request visible to the ScoreFunction. If your ScoreFunction requires information from the client
 *  request to perform scoring, use the {@link org.kiji.scoring.batch.ScoreFunctionJobBuilder} API.
 * </p>
 *
 * <p>
 *   Example usage:
 *   <pre>
 *     kiji score \
 *         --input="format=kiji table=kiji://.env/default/table" \
 *         --output="format=kiji table=kiji://.env/default/table" \
 *         --score-function-class=com.mycompany.scoring.ScoreFunction \
 *         --attached-column=derived:recommendations \
 *         --parameters='{"key":"value"}' \
 *         --interactive=false
 *   </pre>
 * </p>
 */
public class KijiBatchScoreTool extends KijiJobTool<ScoreFunctionJobBuilder> {
  private static final Gson GSON = new Gson();

  @Flag(name="score-function-class",
      usage="Fully qualified class name of the ScoreFunction to run.")
  private String mScoreFunctionClassFlag = null;

  @Flag(name="num-threads", usage="Positive integer number of threads per mapper.")
  private int mNumThreadsPerMapper = 1;

  @Flag(name="attached-column", usage="Set the column to which the ScoreFunction will appear to be "
      + "attached. This column is where output from the ScoreFunction will be written; The schema "
      + "of this column should be compatible with the schema of values returned by the "
      + "ScoreFunction.")
  private String mAttachedColumnFlag = null;

  @Flag(name="parameters", usage="JSON encoded map of string-string parameters which will be "
      + "available to the ScoreFunction.")
  private String mParametersFlag = null;

  private KijiTableMapReduceJobOutput mOutput = null;

  /** {@inheritDoc} */
  @Override
  protected ScoreFunctionJobBuilder createJobBuilder() {
    return ScoreFunctionJobBuilder.create();
  }

  /** {@inheritDoc} */
  @Override
  protected void configure(
      final ScoreFunctionJobBuilder builder
  ) throws IOException, ClassNotFoundException {
    super.configure(builder);

    builder
        .withScoreFunctionClass(
            Class.forName(mScoreFunctionClassFlag).asSubclass(ScoreFunction.class))
        .withOutput(mOutput)
        .withNumThreadsPerMapper(mNumThreadsPerMapper)
        .withAttachedColumn(new KijiColumnName(mAttachedColumnFlag));
    if (null != mParametersFlag) {
      builder.withParameters(GSON.fromJson(mParametersFlag, Map.class));
    }
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "batch-score";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Run a ScoreFunction MapReduce job.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "MapReduce";
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    Preconditions.checkArgument(null != mScoreFunctionClassFlag, "Specify a ScoreFunction class "
        + "with --score-function-class");
    Preconditions.checkArgument(getJobOutput() instanceof KijiTableMapReduceJobOutput,
        "ScoreFunction must output to a Kiji table, but got: {}",
        getJobOutput().getClass().getName());
    Preconditions.checkArgument(null != mAttachedColumnFlag, "Specify an attached column with "
        + "--attached-column");

  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    if (!mayProceed("ScoreFunction MapReduce jobs run from the command line may not override the "
        + "default client data request visible to the ScoreFunction. If your ScoreFunction requires"
        + " information from the client request to perform scoring, use the "
        + "org.kiji.scoring.batch.ScoreFunctionJobBuilder API.")) {
      return BaseTool.FAILURE;
    }
    mOutput = (KijiTableMapReduceJobOutput) getJobOutput();
    final int jobStatus = super.run(nonFlagArgs);
    if (mOutput instanceof DirectKijiTableMapReduceJobOutput) {
      if (0 == jobStatus) {
        getPrintStream().printf("ScoreFunction %s for table %s completed successfully.%n",
            mScoreFunctionClassFlag, mOutput.getOutputTableURI());
      } else {
        getPrintStream().printf("ScoreFunction %s failed. Table %s may have partial writes.",
            mScoreFunctionClassFlag, mOutput.getOutputTableURI());
      }
    } else if (mOutput instanceof HFileMapReduceJobOutput) {
      if (0 == jobStatus) {
        getPrintStream().printf("ScoreFunction %s for table %s completed successfully.%n"
            + "HFiles may now be loaded with: kiji bulk-load --table=%s%n",
            mScoreFunctionClassFlag, mOutput.getOutputTableURI(), mOutput.getOutputTableURI());
      } else {
        getPrintStream().printf("ScoreFunction %s failed. HFiles were not generated.",
            mScoreFunctionClassFlag);
      }
    } else {
      throw new InternalKijiError(
          "ScoreFunction MapReduce job run without Kiji table output.");
    }
    return jobStatus;
  }

  /**
   * Program entry point.
   *
   * @param args command-line arguments.
   * @throws Exception if there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new KijiBatchScoreTool(), args));
  }
}
