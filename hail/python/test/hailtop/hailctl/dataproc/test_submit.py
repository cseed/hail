import pytest

from hailtop import hailctl


def test_submit(gcloud_run):
    hailctl.main(["dataproc", "submit", "test-cluster", "a-script.py"])
    gcloud_args = gcloud_run.call_args[0][0]
    assert gcloud_args[:8] == ["gcloud", "--project=hailctl-dataproc-tests", "dataproc", "--region=us-central1", "jobs", "submit", "pyspark", "a-script.py"]
    assert "--cluster=test-cluster" in gcloud_args


def test_cluster_and_script_required(gcloud_run):
    with pytest.raises(SystemExit):
        hailctl.main(["dataproc", "submit"])

    assert gcloud_run.call_count == 0

    with pytest.raises(SystemExit):
        hailctl.main(["dataproc", "submit", "test-cluster"])

    assert gcloud_run.call_count == 0


def test_dry_run(gcloud_run):
    hailctl.main(["dataproc", "--dry-run", "submit", "test-cluster", "a-script.py"])
    assert gcloud_run.call_count == 0


def test_script_args(gcloud_run):
    hailctl.main(["dataproc", "submit", "test-cluster", "a-script.py", "--", "--foo", "bar"])
    gcloud_args = gcloud_run.call_args[0][0]
    print(gcloud_args)
    job_args = gcloud_args[gcloud_args.index("--") + 1:]
    assert job_args == ["--foo", "bar"]


def test_files(gcloud_run):
    hailctl.main(["dataproc", "submit", "test-cluster", "a-script.py", "--files=some-file.txt"])
    assert "--" not in gcloud_run.call_args[0][0]  # make sure arg is passed to gcloud and not job
    assert "--files=some-file.txt" in gcloud_run.call_args[0][0]


def test_properties(gcloud_run):
    hailctl.main(["dataproc", "submit", "test-cluster", "a-script.py", "--properties=spark:spark.task.maxFailures=3"])
    assert "--" not in gcloud_run.call_args[0][0]  # make sure arg is passed to gcloud and not job
    assert "--properties=spark:spark.task.maxFailures=3" in gcloud_run.call_args[0][0]


def test_gcloud_configuration(gcloud_run):
    hailctl.main(["dataproc", "--gcloud-configuration=some-config", "submit", "test-cluster", "a-script.py"])
    assert "--" not in gcloud_run.call_args[0][0]  # make sure arg is passed to gcloud and not job
    assert "--configuration=some-config" in gcloud_run.call_args[0][0]
