import os

RESOURCES_DIR = "resources"
EXPERIMENTS_DIR = "experiments"
START_PARAMS_DIR = "start_params"

AUTOTESTS_FILE = os.path.join(RESOURCES_DIR, "autotests.xlsx")

RESULTS_FILE = os.path.join(RESOURCES_DIR, "results.xlsx")

BASE_EXPERIMENT_FILE = os.path.join(EXPERIMENTS_DIR, "base_experiment.xlsx")


TREND_PERMISSIBLE_ERROR = 5  # percent
RELATIVE_ERROR = 10  # percent

# development

YEARS_TO_CHECK = [
    2025,
    2026,
]  # Заполнять в зависимости от условий проведения количественных тестов

TYPE_TO_EXECUTION_UUID = {
    "quality": "5e76df9b-836a-4a4d-bd11-1ff544ae30e7",
    "quantity": "5e76df9b-836a-4a4d-bd11-1ff544ae30e7",
}
