# beam-test

Created to test Beam 2.36 null values after join. We noticed this in Scio's [join](https://github.com/spotify/scio/blob/main/scio-core/src/main/scala/com/spotify/scio/util/ArtisanJoin.scala#L103)
implementation and were able to confirm it happening with Beam CGBK primitive, too.

To run the job:

```
sbt "runMain clairem.data.JoinTest --runner=DataflowRunner --region=us-central1 --project=[[PROJECT]]"
```

You can also run it using the equivalent Scio join API with:
```
sbt "runMain clairem.data.JoinTest --runner=DataflowRunner --region=us-central1 --project=[[PROJECT]] --impl=scio"
```

In both cases you get this error in the Dataflow logs:

```
2022-03-15 10:35:51.489 EDTError message from worker: java.lang.RuntimeException: org.apache.beam.sdk.util.UserCodeException: java.lang.IllegalArgumentException: Unable to encode element '[org.apache.beam.sdk.transforms.join.CoGbkResult$TagIterable@33dd2cbb, org.apache.beam.sdk.transforms.join.CoGbkResult$TagIterable@26edfb82]' with coder 'org.apache.beam.sdk.transforms.join.CoGbkResult$CoGbkResultCoder@346927'. org.apache.beam.runners.dataflow.worker.GroupAlsoByWindowsParDoFn$1.output(GroupAlsoByWindowsParDoFn.java:187)
org.apache.beam.runners.dataflow.worker.GroupAlsoByWindowFnRunner$1.outputWindowedValue(GroupAlsoByWindowFnRunner.java:108)
org.apache.beam.runners.dataflow.worker.util.BatchGroupAlsoByWindowViaIteratorsFn.processElement(BatchGroupAlsoByWindowViaIteratorsFn.java:128)
org.apache.beam.runners.dataflow.worker.util.BatchGroupAlsoByWindowViaIteratorsFn.processElement(BatchGroupAlsoByWindowViaIteratorsFn.java:56)
org.apache.beam.runners.dataflow.worker.GroupAlsoByWindowFnRunner.invokeProcessElement(GroupAlsoByWindowFnRunner.java:121)
org.apache.beam.runners.dataflow.worker.GroupAlsoByWindowFnRunner.processElement(GroupAlsoByWindowFnRunner.java:73)
org.apache.beam.runners.dataflow.worker.GroupAlsoByWindowsParDoFn.processElement(GroupAlsoByWindowsParDoFn.java:117)
org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoOperation.process(ParDoOperation.java:44)
org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver.process(OutputReceiver.java:49)
org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation.runReadLoop(ReadOperation.java:212)
org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation.start(ReadOperation.java:163)
org.apache.beam.runners.dataflow.worker.util.common.worker.MapTaskExecutor.execute(MapTaskExecutor.java:83)
org.apache.beam.runners.dataflow.worker.BatchDataflowWorker.executeWork(BatchDataflowWorker.java:420)
org.apache.beam.runners.dataflow.worker.BatchDataflowWorker.doWork(BatchDataflowWorker.java:389)
org.apache.beam.runners.dataflow.worker.BatchDataflowWorker.getAndPerformWork(BatchDataflowWorker.java:314)
org.apache.beam.runners.dataflow.worker.DataflowBatchWorkerHarness$WorkerThread.doWork(DataflowBatchWorkerHarness.java:140)
org.apache.beam.runners.dataflow.worker.DataflowBatchWorkerHarness$WorkerThread.call(DataflowBatchWorkerHarness.java:120)
org.apache.beam.runners.dataflow.worker.DataflowBatchWorkerHarness$WorkerThread.call(DataflowBatchWorkerHarness.java:107)
java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
java.base/java.lang.Thread.run(Thread.java:834)
Caused by: org.apache.beam.sdk.util.UserCodeException: java.lang.IllegalArgumentException: Unable to encode element '[org.apache.beam.sdk.transforms.join.CoGbkResult$TagIterable@33dd2cbb, org.apache.beam.sdk.transforms.join.CoGbkResult$TagIterable@26edfb82]' with coder 'org.apache.beam.sdk.transforms.join.CoGbkResult$CoGbkResultCoder@346927'. org.apache.beam.sdk.util.UserCodeException.wrap(UserCodeException.java:39)
org.apache.beam.sdk.transforms.join.CoGroupByKey$ConstructCoGbkResultFn$DoFnInvoker.invokeProcessElement(Unknown Source)
org.apache.beam.runners.dataflow.worker.repackaged.org.apache.beam.runners.core.SimpleDoFnRunner.invokeProcessElement(SimpleDoFnRunner.java:228)
org.apache.beam.runners.dataflow.worker.repackaged.org.apache.beam.runners.core.SimpleDoFnRunner.processElement(SimpleDoFnRunner.java:187)
org.apache.beam.runners.dataflow.worker.SimpleParDoFn.processElement(SimpleParDoFn.java:340)
org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoOperation.process(ParDoOperation.java:44)
org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver.process(OutputReceiver.java:49)
org.apache.beam.runners.dataflow.worker.GroupAlsoByWindowsParDoFn$1.output(GroupAlsoByWindowsParDoFn.java:185)
... 21 more Caused by: java.lang.IllegalArgumentException: Unable to encode element '[org.apache.beam.sdk.transforms.join.CoGbkResult$TagIterable@33dd2cbb, org.apache.beam.sdk.transforms.join.CoGbkResult$TagIterable@26edfb82]' with coder 'org.apache.beam.sdk.transforms.join.CoGbkResult$CoGbkResultCoder@346927'. org.apache.beam.sdk.coders.Coder.getEncodedElementByteSize(Coder.java:300)
org.apache.beam.sdk.coders.Coder.registerByteSizeObserver(Coder.java:291)
org.apache.beam.sdk.coders.KvCoder.registerByteSizeObserver(KvCoder.java:130)
org.apache.beam.sdk.coders.KvCoder.registerByteSizeObserver(KvCoder.java:37)
org.apache.beam.sdk.util.WindowedValue$FullWindowedValueCoder.registerByteSizeObserver(WindowedValue.java:642)
org.apache.beam.sdk.util.WindowedValue$FullWindowedValueCoder.registerByteSizeObserver(WindowedValue.java:558)
org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory$ElementByteSizeObservableCoder.registerByteSizeObserver(IntrinsicMapTaskExecutorFactory.java:403)
org.apache.beam.runners.dataflow.worker.util.common.worker.OutputObjectAndByteCounter.update(OutputObjectAndByteCounter.java:128)
org.apache.beam.runners.dataflow.worker.DataflowOutputCounter.update(DataflowOutputCounter.java:67)
org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver.process(OutputReceiver.java:43)
org.apache.beam.runners.dataflow.worker.SimpleParDoFn$1.output(SimpleParDoFn.java:285)
org.apache.beam.runners.dataflow.worker.repackaged.org.apache.beam.runners.core.SimpleDoFnRunner.outputWindowedValue(SimpleDoFnRunner.java:268)
org.apache.beam.runners.dataflow.worker.repackaged.org.apache.beam.runners.core.SimpleDoFnRunner.access$900(SimpleDoFnRunner.java:84)
org.apache.beam.runners.dataflow.worker.repackaged.org.apache.beam.runners.core.SimpleDoFnRunner$DoFnProcessContext.output(SimpleDoFnRunner.java:416)
org.apache.beam.runners.dataflow.worker.repackaged.org.apache.beam.runners.core.SimpleDoFnRunner$DoFnProcessContext.output(SimpleDoFnRunner.java:404)
org.apache.beam.sdk.transforms.join.CoGroupByKey$ConstructCoGbkResultFn.processElement(CoGroupByKey.java:192)
Caused by: org.apache.beam.sdk.coders.CoderException: cannot encode a null String org.apache.beam.sdk.coders.StringUtf8Coder.encode(StringUtf8Coder.java:74)
org.apache.beam.sdk.coders.StringUtf8Coder.encode(StringUtf8Coder.java:68)
org.apache.beam.sdk.coders.StringUtf8Coder.encode(StringUtf8Coder.java:37)
org.apache.beam.sdk.coders.IterableLikeCoder.encode(IterableLikeCoder.java:125)
org.apache.beam.sdk.transforms.join.CoGbkResult$CoGbkResultCoder.encode(CoGbkResult.java:268)
org.apache.beam.sdk.transforms.join.CoGbkResult$CoGbkResultCoder.encode(CoGbkResult.java:229)
org.apache.beam.sdk.coders.Coder.getEncodedElementByteSize(Coder.java:297)
org.apache.beam.sdk.coders.Coder.registerByteSizeObserver(Coder.java:291)
org.apache.beam.sdk.coders.KvCoder.registerByteSizeObserver(KvCoder.java:130)
org.apache.beam.sdk.coders.KvCoder.registerByteSizeObserver(KvCoder.java:37)
org.apache.beam.sdk.util.WindowedValue$FullWindowedValueCoder.registerByteSizeObserver(WindowedValue.java:642)
org.apache.beam.sdk.util.WindowedValue$FullWindowedValueCoder.registerByteSizeObserver(WindowedValue.java:558)
org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory$ElementByteSizeObservableCoder.registerByteSizeObserver(IntrinsicMapTaskExecutorFactory.java:403)
org.apache.beam.runners.dataflow.worker.util.common.worker.OutputObjectAndByteCounter.update(OutputObjectAndByteCounter.java:128)
org.apache.beam.runners.dataflow.worker.DataflowOutputCounter.update(DataflowOutputCounter.java:67)
org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver.process(OutputReceiver.java:43)
org.apache.beam.runners.dataflow.worker.SimpleParDoFn$1.output(SimpleParDoFn.java:285)
org.apache.beam.runners.dataflow.worker.repackaged.org.apache.beam.runners.core.SimpleDoFnRunner.outputWindowedValue(SimpleDoFnRunner.java:268)
org.apache.beam.runners.dataflow.worker.repackaged.org.apache.beam.runners.core.SimpleDoFnRunner.access$900(SimpleDoFnRunner.java:84)
org.apache.beam.runners.dataflow.worker.repackaged.org.apache.beam.runners.core.SimpleDoFnRunner$DoFnProcessContext.output(SimpleDoFnRunner.java:416)
org.apache.beam.runners.dataflow.worker.repackaged.org.apache.beam.runners.core.SimpleDoFnRunner$DoFnProcessContext.output(SimpleDoFnRunner.java:404)
org.apache.beam.sdk.transforms.join.CoGroupByKey$ConstructCoGbkResultFn.processElement(CoGroupByKey.java:192)
org.apache.beam.sdk.transforms.join.CoGroupByKey$ConstructCoGbkResultFn$DoFnInvoker.invokeProcessElement(Unknown Source)
org.apache.beam.runners.dataflow.worker.repackaged.org.apache.beam.runners.core.SimpleDoFnRunner.invokeProcessElement(SimpleDoFnRunner.java:228)
org.apache.beam.runners.dataflow.worker.repackaged.org.apache.beam.runners.core.SimpleDoFnRunner.processElement(SimpleDoFnRunner.java:187)
org.apache.beam.runners.dataflow.worker.SimpleParDoFn.processElement(SimpleParDoFn.java:340)
org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoOperation.process(ParDoOperation.java:44)
org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver.process(OutputReceiver.java:49)
org.apache.beam.runners.dataflow.worker.GroupAlsoByWindowsParDoFn$1.output(GroupAlsoByWindowsParDoFn.java:185)
org.apache.beam.runners.dataflow.worker.GroupAlsoByWindowFnRunner$1.outputWindowedValue(GroupAlsoByWindowFnRunner.java:108)
org.apache.beam.runners.dataflow.worker.util.BatchGroupAlsoByWindowViaIteratorsFn.processElement(BatchGroupAlsoByWindowViaIteratorsFn.java:128)
org.apache.beam.runners.dataflow.worker.util.BatchGroupAlsoByWindowViaIteratorsFn.processElement(BatchGroupAlsoByWindowViaIteratorsFn.java:56)
org.apache.beam.runners.dataflow.worker.GroupAlsoByWindowFnRunner.invokeProcessElement(GroupAlsoByWindowFnRunner.java:121)
org.apache.beam.runners.dataflow.worker.GroupAlsoByWindowFnRunner.processElement(GroupAlsoByWindowFnRunner.java:73)
org.apache.beam.runners.dataflow.worker.GroupAlsoByWindowsParDoFn.processElement(GroupAlsoByWindowsParDoFn.java:117)
org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoOperation.process(ParDoOperation.java:44)
org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver.process(OutputReceiver.java:49)
org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation.runReadLoop(ReadOperation.java:212)
org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation.start(ReadOperation.java:163)
org.apache.beam.runners.dataflow.worker.util.common.worker.MapTaskExecutor.execute(MapTaskExecutor.java:83)
org.apache.beam.runners.dataflow.worker.BatchDataflowWorker.executeWork(BatchDataflowWorker.java:420)
org.apache.beam.runners.dataflow.worker.BatchDataflowWorker.doWork(BatchDataflowWorker.java:389)
org.apache.beam.runners.dataflow.worker.BatchDataflowWorker.getAndPerformWork(BatchDataflowWorker.java:314)
org.apache.beam.runners.dataflow.worker.DataflowBatchWorkerHarness$WorkerThread.doWork(DataflowBatchWorkerHarness.java:140)
org.apache.beam.runners.dataflow.worker.DataflowBatchWorkerHarness$WorkerThread.call(DataflowBatchWorkerHarness.java:120)
org.apache.beam.runners.dataflow.worker.DataflowBatchWorkerHarness$WorkerThread.call(DataflowBatchWorkerHarness.java:107)
java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
java.base/java.lang.Thread.run(Thread.java:834)
```

You can run it using Beam 2.35.0 by updating `build.sbt` to set `beamVersion` to `2.35.0` *and* `scioVersion` to `0.11.4` (Scio version must correspond to Beam version).


This project is based on the [scio.g8](https://github.com/spotify/scio.g8).
