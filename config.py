import os

RESOURCES_DIR = "resources"
EXPERIMENTS_DIR = "experiments"

AUTOTESTS_FILE = os.path.join(RESOURCES_DIR, "autotests.xlsx")
START_PARAMS_FILE = os.path.join(RESOURCES_DIR, "start_params.xlsx")

RESULTS_FILE = os.path.join(RESOURCES_DIR, "results.xlsx")

BASE_EXPERIMENT_FILE = os.path.join(EXPERIMENTS_DIR, "base_experiment.xlsx")


TREND_PERMISSIBLE_ERROR = 5
RELATIVE_ERROR = 10

# development

TYPE_TO_EXECUTION_UUID = {
    "quality": "5e76df9b-836a-4a4d-bd11-1ff544ae30e7",
    "quantity": "5e76df9b-836a-4a4d-bd11-1ff544ae30e7"
}