import os
import re
import asyncio
import sys
from openpyxl import load_workbook
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
    pattern = r"experiments\\results_(?P<execution_uuid>[a-zA-Z0-9-]+)_(?P<experiment_id>[a-zA-Z0-9-]+).xlsx"

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
        r"experiments\\results_(?P<execution_uuid>[a-zA-Z0-9-]+)_(?P<experiment_id>[a-zA-Z0-9-]+)\\.xlsx"
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


def update_excel_cell(sheet, row, col, value):
    """Обновляет значение ячейки в листе Excel."""
    sheet.cell(row=row, column=col, value=value)


def process_tests_common(
    tests_df, workbook, sheet_name, prefix, uuid_key, process_test
):
    """Общий процесс обработки тестов."""
    print(prefix, f"Началась обработка {prefix.lower()} тестов...")
    files_list = load_results_files()
    execution_uuid = get_uuid_by_type(uuid_key)
    base_file = get_file_path_by_execution_uuid_and_experiment_id(
        files_list, execution_uuid, "0"
    )
    base_df = pd.read_excel(base_file)

    results = []
    for index, test in tests_df.iterrows():
        experiment_id = str(test["id Теста"])

        print(prefix, f"PROCESS Experiment {experiment_id}")
        result = process_test(test, base_df, files_list, execution_uuid, experiment_id)

        sheet = workbook[sheet_name]

        # Update result in Excel
        update_excel_cell(
            sheet, index + 2, tests_df.columns.get_loc("Итог") + 1, result
        )
        for col_idx, value in enumerate(test):
            sheet.cell(row=index + 2, column=col_idx + 1, value=value)

        results.append(
            {
                "execution_uuid": execution_uuid,
                "experiment_id": experiment_id,
                "result": result,
            }
        )

    return results


def process_qualitative_test(test, base_df, files_list, execution_uuid, experiment_id):
    """Обрабатывает один качественный тест."""
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
            # >|(id=14)|
            linked_sign = linkage[0]
            linked_id = linkage.split("id=")[1].split(")")[0]
            linked_file = get_file_path_by_execution_uuid_and_experiment_id(
                files_list, execution_uuid, linked_id
            )
            linked_df = pd.read_excel(linked_file)

            linked_sum = linked_df["sum"].sum()
            current_sum = experiment_df["sum"].sum()

            linkage_test_result = False
            if linked_sign == ">":
                linkage_test_result = current_sum > linked_sum
            elif linked_sign == "<":
                linkage_test_result = current_sum < linked_sum
            else:
                print(
                    QUALITY_PREFIX,
                    f"-- ERROR: Непредусмотренный символ во взаимосвязи расчетов '{linked_sign}'",
                )
            print(
                QUALITY_PREFIX,
                f"-- {'SUCCESS' if linkage_test_result else 'FAILED'}: linkage test {linkage}",
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
                        f"-- FAILED: trend test for year {year}, difference {difference} (expected trend {expected_trend})",
                    )
                    trend_test_result = False
                    break
            else:
                difference_percent = abs(base_value - compare_value) / base_value * 100
                if difference_percent > TREND_PERMISSIBLE_ERROR:
                    print(
                        QUALITY_PREFIX,
                        f"-- FAILED: trend test for year {year}, difference {difference_percent}%",
                    )
                    trend_test_result = False
                    break

            print(QUALITY_PREFIX, f"-- SUCCESS: trend test for year {year}")

        result = linkage_test_result and trend_test_result

    return result


def process_quantitative_test(test, base_df, files_list, execution_uuid, experiment_id):
    """Обрабатывает один количественный тест."""
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
        temp_df["difference"] = experiment_df["sum"] - base_df["sum"]
        temp_df["year"] = pd.to_datetime(temp_df["dt"]).dt.year

        result = True

        for year in YEARS_TO_CHECK:
            effect_tnav = test[f"Эффект за {year} год по tNav"]
            effect_ml = temp_df.loc[temp_df["year"] == year, "difference"].values[0]
            relative_error = (1 - (effect_tnav - effect_ml) / effect_tnav) * 100
            test[f"Эффект за {year} год по ML"] = effect_ml

            if abs(relative_error) > RELATIVE_ERROR:
                print(
                    QUANTITY_PREFIX,
                    f"-- FAILED: quantitative test for year {year}: relative error {abs(relative_error)} over the limit {RELATIVE_ERROR}%",
                )
                result = False
            else:
                print(QUANTITY_PREFIX, f"-- SUCCESS: quantitative test for year {year}")
        # effect_tnav = test["Эффект за 2025-2026 года по tNav"]
        # effect_ml = sum(
        #     temp_df.loc[temp_df["year"] == year, "difference"].values[0]
        #     for year in YEARS_TO_CHECK
        # )

    return result


async def process_qualitative_tests(qualitative_df: pd.DataFrame, workbook) -> list:
    return process_tests_common(
        qualitative_df,
        workbook,
        "Список качественных автотестов",
        QUALITY_PREFIX,
        "quality",
        process_qualitative_test,
    )


async def process_quantitative_tests(quantitative_df: pd.DataFrame, workbook) -> list:
    return process_tests_common(
        quantitative_df,
        workbook,
        "Список количественных автотесто",
        QUANTITY_PREFIX,
        "quantity",
        process_quantitative_test,
    )


def process_tests(qualitative_df, quantitative_df, workbook) -> tuple:
    async def _run_async_process():
        tasks = [
            process_qualitative_tests(qualitative_df, workbook),
            process_quantitative_tests(quantitative_df, workbook),
        ]
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

    workbook = load_workbook(AUTOTESTS_FILE)
    qualitative_df = pd.read_excel(
        AUTOTESTS_FILE, sheet_name="Список качественных автотестов", engine="openpyxl"
    )
    quantitative_df = pd.read_excel(
        AUTOTESTS_FILE, sheet_name="Список количественных автотесто", engine="openpyxl"
    )

    # Обработка качественных и количественных тестов
    process_tests(qualitative_df, quantitative_df, workbook)

    # Сохранение изменений
    workbook.save(AUTOTESTS_FILE)


if __name__ == "__main__":
    try:
        run_stage_two()
    except Exception as e:
        print(f"Ошибка выполнения второго этапа: {e}")
    finally:
        sys.exit(1)
