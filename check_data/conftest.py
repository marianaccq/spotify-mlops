"""
Creator: Mariana
Date: Fev. 2022
Download the raw data
"""
import pytest
import pandas as pd
import wandb

run = wandb.init(job_type="data_tests")

def pytest_addoption(parser):
    parser.addoption("--reference_artifact", action="store")


@pytest.fixture(scope="session")
def data(request):
    reference_artifact = request.config.option.reference_artifact

    if reference_artifact is None:
        pytest.fail("--reference_artifact missing on command line")

    local_path = run.use_artifact(reference_artifact).file()
    sample1 = pd.read_csv(local_path)

    return sample1