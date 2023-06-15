# beam-test

Created to test Beam features.

### Runner V2 OOM Test

To run the job with RunnerV2, resulting in an OOM:

```
sbt "runMain clairem.data.RunnerV2Test --runner=DataflowRunner --region=us-central1 --project=[[PROJECT]] --experiments=use_runner_v2 --n=10"
```

To run the job with Runner Classic, which succeeds:

```
sbt "runMain clairem.data.RunnerV2Test --runner=DataflowRunner --region=us-central1 --project=[[PROJECT]] --n=10"
```

This project is based on the [scio.g8](https://github.com/spotify/scio.g8).
