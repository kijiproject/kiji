/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.mapreduce;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

public class TestMapReduceJob {
  /** A concrete implementation of the abstract MapReduceJob that we can test. */
  public static class ConcreteMapReduceJob extends MapReduceJob {
    public ConcreteMapReduceJob(Job job) {
      super(job);
    }
  }

  @Test
  public void testGetHadoopJob() {
    Job hadoopJob = createMock(Job.class);

    replay(hadoopJob);

    MapReduceJob job = new ConcreteMapReduceJob(hadoopJob);
    assertTrue(hadoopJob == job.getHadoopJob());

    verify(hadoopJob);
  }

  @Test
  public void testRun() throws ClassNotFoundException, IOException, InterruptedException {
    Job hadoopJob = createMock(Job.class);

    // Expect that the job is run and that it is successful.
    expect(hadoopJob.waitForCompletion(true)).andReturn(true);

    replay(hadoopJob);

    MapReduceJob job = new ConcreteMapReduceJob(hadoopJob);
    assertTrue(job.run());

    verify(hadoopJob);
  }

  @Test
  public void testSubmit() throws ClassNotFoundException, IOException, InterruptedException {
    Job hadoopJob = createMock(Job.class);

    // Expect that the job is submitted and that it is successful.
    hadoopJob.submit();
    expect(hadoopJob.isComplete()).andReturn(false);
    expect(hadoopJob.isComplete()).andReturn(true);
    expect(hadoopJob.isSuccessful()).andReturn(true);

    replay(hadoopJob);

    MapReduceJob job = new ConcreteMapReduceJob(hadoopJob);
    MapReduceJob.Status jobStatus = job.submit();
    assertFalse(jobStatus.isComplete());
    assertTrue(jobStatus.isComplete());
    assertTrue(jobStatus.isSuccessful());

    verify(hadoopJob);
  }
}
