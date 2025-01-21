import asyncio
import pandas as pd
from datetime import datetime
import os
import sys
import re

from config import START_PARAMS_DIR, AUTOTESTS_FILE, RESOURCES_DIR


def process_years(years):
    if isinstance(years, int):
        return [years]
    if isinstance(years, str):
        return [int(year.strip()) for year in years.split(";")]
    raise ValueError(f"Неверный формат данных для года: {years}")


def process_events(event_str):
    """
    Парсит строку с мероприятиями в формат [(ID, параметр), ...].
    Учитывает разделители `;` и перенос строки.
    """
    if pd.isna(event_str):
        return []

    events = re.split(r"[;\n]+", event_str.strip())

    parsed_events = []
    for event in events:
        match = re.match(r"\((\d+),\s*(\d+)\)", event.strip())
        if match:
            parsed_events.append((int(match.group(1)), int(match.group(2))))

    return parsed_events


async def generate_quality_tests():
    tests_df = pd.read_excel(
        AUTOTESTS_FILE, sheet_name="Список качественных автотестов"
    )

    order_number = 1
    for _, row in tests_df.iterrows():
        df = pd.DataFrame(
            columns=[
                "Порядковый номер",
                "ID мероприятия",
                "Параметры мероприятия",
                "Дата проведения",
                "Название",
                "Статус",
            ]
        )
        try:
            events = process_events(row["Мероприятие"])
            years = process_years(row["Год"])

            for i in range(len(years)):
                event_id, event_params = events[i]

                df.loc[len(df)] = {
                    "Порядковый номер": order_number,
                    "ID мероприятия": event_id,
                    "Название": event_id,
                    "Дата проведения": datetime(years[i], 1, 1),
                    "Параметры мероприятия": event_params,
                    "Статус": True,
                }

            file_name = os.path.join(
                START_PARAMS_DIR, f"quality_test_{order_number}.xlsx"
            )
            df.to_excel(file_name, index=False)
            print(f"Файл {file_name} успешно создан.")

            order_number += 1
        except Exception as e:
            raise Exception(f"Ошибка обработки строки: {row}. Детали: {e}")


async def generate_quantity_tests():
    tests_df = pd.read_excel(
        AUTOTESTS_FILE, sheet_name="Список количественных автотесто"
    )

    order_number = 1
    for _, row in tests_df.iterrows():
        df = pd.DataFrame(
            columns=[
                "Порядковый номер",
                "ID мероприятия",
                "Параметры мероприятия",
                "Дата проведения",
                "Название",
                "Статус",
            ]
        )
        try:
            events = process_events(row["Мероприятие"])

            for event in events:
                event_id, event_params = event

                df.loc[len(df)] = {
                    "Порядковый номер": order_number,
                    "ID мероприятия": event_id,
                    "Название": event_id,
                    "Дата проведения": pd.to_datetime(row["Год запуска"]),
                    "Параметры мероприятия": event_params,
                    "Статус": True,
                }

            file_name = os.path.join(
                START_PARAMS_DIR, f"quantity_test_{order_number}.xlsx"
            )
            df.to_excel(file_name, index=False)
            print(f"Файл {file_name} успешно создан.")

            order_number += 1
        except Exception as e:
            raise Exception(f"Ошибка обработки строки: {row}. Детали: {e}")


def process_generation():
    async def _run_async_process():
        tasks = [
            generate_quality_tests(),
            generate_quantity_tests(),
        ]
        return await asyncio.gather(*tasks)

    asyncio.run(_run_async_process())


def run_stage_one():
    if not os.path.exists(AUTOTESTS_FILE):
        raise FileNotFoundError(
            f"Файл {AUTOTESTS_FILE} не найден. Поместите его в папку {RESOURCES_DIR}."
        )

    if not os.path.exists(START_PARAMS_DIR):
        raise FileNotFoundError(
            f"Директория {START_PARAMS_DIR} не найдена. Создайте её."
        )

    process_generation()

    print("Генерация файлов со стартовыми параметрами завершена.")


if __name__ == "__main__":
    try:
        run_stage_one()
    except Exception as e:
        print(f"Ошибка выполнения первого этапа: {e}")
        sys.exit(1)
