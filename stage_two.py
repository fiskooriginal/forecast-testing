import os
import sys
import re

import asyncio

import pandas as pd

from config import (
    RESULTS_FILE,
    AUTOTESTS_FILE,
    EXPERIMENTS_DIR,
    TREND_PERMISSIBLE_ERROR,
    RELATIVE_ERROR,
    YEARS_TO_CHECK,
    TYPE_TO_EXECUTION_UUID,
)

QUANTITY_PREFIX = "[QUANTITY] "
QUALITY_PREFIX = "[QUALITY] "


def get_uuid_by_type(type: str) -> str:
    uuid = TYPE_TO_EXECUTION_UUID.get(type)
    if uuid is None:
        raise Exception(f"Нарушена связь TYPE to UUID для теста №{type}")
    return uuid


def get_file_path_by_execution_uuid_and_experiment_id(
    files: list, execution_uuid: str, experiment_id: str
):
    pattern = r"experiments\\results_(?P<execution_uuid>[a-zA-Z0-9-]+)_(?P<experiment_id>[a-zA-Z0-9-]+)\.xlsx"

    for file_path in files:
        match = re.match(pattern, file_path)
        if match:
            if (
                match.group("execution_uuid") == execution_uuid
                and match.group("experiment_id") == experiment_id
            ):
                return file_path

    return None


def filter_and_sort_files_by_execution_uuid(files, target_execution_uuid):
    pattern = re.compile(
        r"experiments\\results_(?P<execution_uuid>[a-zA-Z0-9-]+)_(?P<experiment_id>[a-zA-Z0-9-]+)\.xlsx"
    )

    filtered_files = [
        (match.group("experiment_id"), file)
        for file in files
        if (match := pattern.match(file))
        and match.group("execution_uuid") == target_execution_uuid
    ]

    filtered_files.sort(key=lambda x: x[0])

    return [file for _, file in filtered_files]


def prepare_trend_conditions(trend: str) -> list:
    result = []

    trend_conditions = [t.strip("() ").split(":") for t in trend.split(";")]

    for condition in trend_conditions:
        years_str, value = condition

        if "-" in years_str:
            start_year, end_year = map(int, years_str.split("-"))
            years = list(range(start_year, end_year + 1))
        else:
            years = [int(years_str)]

        value = int(value)

        for year in years:
            result.append((year, value))

    return result


def load_results_files() -> list:
    """Загружает все файлы результатов из папки экспериментов."""

    results_files = [
        os.path.join(EXPERIMENTS_DIR, file)
        for file in os.listdir(EXPERIMENTS_DIR)
        if file.startswith("results_") and file.endswith(".xlsx")
    ]

    if not results_files:
        raise FileNotFoundError(
            f"Не найдено ни одного файла с результатами в директории /{EXPERIMENTS_DIR}"
        )

    return results_files


def calculate_trend(base_value, compare_value) -> tuple:
    """Определяет тренд между двумя значениями (увеличение, уменьшение, неизменность)."""
    difference = compare_value - base_value

    trend = None
    if compare_value == base_value:
        trend = 0
    elif compare_value > base_value:
        trend = 1
    elif compare_value < base_value:
        trend = -1

    return difference, trend


async def process_qualitative_tests() -> list:
    """Обрабатывает качественные тесты."""
    print(QUALITY_PREFIX, "Началась обработка качественных тестов...")

    files_list = load_results_files()
    execution_uuid = get_uuid_by_type("quality")
    base_file = get_file_path_by_execution_uuid_and_experiment_id(
        files_list, execution_uuid, "0"
    )
    base_df = pd.read_excel(base_file)

    qualitative_df = pd.read_excel(
        AUTOTESTS_FILE, sheet_name="Список качественных автотестов"
    )

    qualitative_results = []
    for _, test in qualitative_df.iterrows():
        experiment_id = str(test["id Теста"])

        print(QUALITY_PREFIX, f"PROCESS Experiment {experiment_id}")
        result = True

        experiment_file = get_file_path_by_execution_uuid_and_experiment_id(
            files_list, execution_uuid, experiment_id
        )
        if experiment_file is None:
            print(
                QUALITY_PREFIX,
                f"-- ERROR: Не найден файл с результатами эксперимента для №{experiment_id}",
            )
            result = False
        else:
            experiment_df = pd.read_excel(experiment_file)

            # Проверка "Взаимосвязь расчетов"
            linkage_test_result = True
            linkage = test["Взаимосвязь расчетов"]
            if linkage and pd.notna(linkage):
                linked_id = linkage.split("id=")[1].split(")")[0]
                linked_file = get_file_path_by_execution_uuid_and_experiment_id(
                    files_list, execution_uuid, linked_id
                )
                linked_df = pd.read_excel(linked_file)

                linked_sum = linked_df["sum"].sum()
                current_sum = experiment_df["sum"].sum()

                if current_sum >= linked_sum:
                    print(
                        QUALITY_PREFIX,
                        f"-- FAILED: experiment #{experiment_id} (linkage {linkage})",
                    )
                    linkage_test_result = False
                    break

                print(
                    QUALITY_PREFIX,
                    f"-- SUCCESS: experiment #{experiment_id} (linkage {linkage})",
                )

            # Проверка "Тренд"
            base_df["year"] = pd.to_datetime(base_df["dt"]).dt.year
            experiment_df["year"] = pd.to_datetime(experiment_df["dt"]).dt.year

            trend_conditions = prepare_trend_conditions(test["Тренд"])

            trend_test_result = True
            for year, expected_trend in trend_conditions:
                base_value = base_df.loc[base_df["year"] == year, "sum"].values[0]
                compare_value = experiment_df.loc[
                    experiment_df["year"] == year, "sum"
                ].values[0]

                if expected_trend != 0:
                    difference, trend = calculate_trend(base_value, compare_value)
                    if trend != expected_trend:
                        print(
                            QUALITY_PREFIX,
                            f"-- FAILED: for year {year}, difference {difference} (expected trend {expected_trend})",
                        )
                        trend_test_result = False
                        break
                else:
                    difference_percent = (
                        abs(base_value - compare_value) / base_value * 100
                    )
                    if difference_percent > TREND_PERMISSIBLE_ERROR:
                        print(
                            QUALITY_PREFIX,
                            f"-- FAILED: for year {year}, difference {difference_percent}%",
                        )
                        trend_test_result = False
                        break

                print(QUALITY_PREFIX, f"-- SUCCESS: for year {year}")

        result = True if linkage_test_result and trend_test_result else False
        # Добавляем результат, независимо от того, прошел ли тест
        qualitative_results.append(
            {
                "execution_uuid": execution_uuid,
                "experiment_id": experiment_id,
                "result": result,
            }
        )

    return qualitative_results


async def process_quantitative_tests() -> list:
    """Обрабатывает количественные тесты."""
    print(QUANTITY_PREFIX, "Началась обработка количественных тестов...")

    files_list = load_results_files()
    execution_uuid = get_uuid_by_type("quantity")
    base_file = get_file_path_by_execution_uuid_and_experiment_id(
        files_list, execution_uuid, "0"
    )
    base_df = pd.read_excel(base_file)

    quantitative_df = pd.read_excel(
        AUTOTESTS_FILE, sheet_name="Список количественных автотесто"
    )

    quantitative_results = []
    for _, test in quantitative_df.iterrows():
        experiment_id = str(test["id Теста"])

        print(QUANTITY_PREFIX, f"Experiment {experiment_id}")
        result = True

        experiment_file = get_file_path_by_execution_uuid_and_experiment_id(
            files_list, execution_uuid, experiment_id
        )
        if experiment_file is None:
            print(
                QUANTITY_PREFIX,
                f"-- ERROR: Не найден файл с результатами эксперимента для №{experiment_id}",
            )
            result = False
        else:
            experiment_df = pd.read_excel(experiment_file)

            temp_df = experiment_df.copy()
            difference = experiment_df["sum"] - base_df["sum"]
            temp_df["difference"] = difference
            temp_df["year"] = pd.to_datetime(temp_df["dt"]).dt.year

            effect_tnav = test["Эффект за 2025-2026 года по tNav"]

            effect_ml = 0
            for year in YEARS_TO_CHECK:
                effect_ml += temp_df.loc[temp_df["year"] == year, "difference"].values[
                    0
                ]

            relative_error = (1 - (effect_tnav - effect_ml) / effect_tnav) * 100

            if abs(relative_error) > RELATIVE_ERROR:
                print(
                    QUANTITY_PREFIX,
                    f"-- ERROR: относительная погрешность {abs(relative_error)} больше допустимой нормы {RELATIVE_ERROR}",
                )
                result = False
            else:
                print(QUANTITY_PREFIX, f"-- SUCCESS: for experiment {experiment_id}")

        quantitative_results.append(
            {
                "execution_uuid": execution_uuid,
                "experiment_id": experiment_id,
                "result": result,
            }
        )

    return quantitative_results


def process_tests() -> tuple:
    async def _run_async_process():
        tasks = [process_qualitative_tests(), process_quantitative_tests()]
        return await asyncio.gather(*tasks)

    return tuple(asyncio.run(_run_async_process()))


def run_stage_two():
    """
    Выполняет второй этап тестирования.
    """
    if not os.path.exists(EXPERIMENTS_DIR):
        raise FileNotFoundError(
            f"Директория {EXPERIMENTS_DIR} с результатами экспериментов не найдена."
        )

    # Обработка качественных и количественных тестов
    qualitative_results, quantitative_results = process_tests()

    print("Завершено автоматическое тестирование")

    # Проверяем, что данные найдены
    if not qualitative_results and not quantitative_results:
        raise ValueError(
            "Результаты экспериментов не найдены или не содержат необходимых данных."
        )

    # Сохранение в RESULTS_FILE только при успешной обработке
    with pd.ExcelWriter(RESULTS_FILE) as writer:
        if qualitative_results:  # Если есть результаты качественных тестов
            qualitative_df = pd.DataFrame(qualitative_results)
            qualitative_df.to_excel(
                writer, sheet_name="Качественные тесты", index=False
            )

        if quantitative_results:  # Если есть результаты количественных тестов
            quantitative_df = pd.DataFrame(quantitative_results)
            quantitative_df.to_excel(
                writer, sheet_name="Количественные тесты", index=False
            )

    print(f"Файл {RESULTS_FILE} успешно создан.")


if __name__ == "__main__":
    try:
        run_stage_two()
    except Exception as e:
        print(f"Ошибка выполнения второго этапа: {e}")
    finally:
        sys.exit(1)
