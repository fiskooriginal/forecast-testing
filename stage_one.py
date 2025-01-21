import pandas as pd
from datetime import datetime
import os
import sys
import re

from config import START_PARAMS_FILE, AUTOTESTS_FILE, RESOURCES_DIR, ANALYTICS_URL

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

def run_stage_one():
    if not os.path.exists(AUTOTESTS_FILE):
        raise FileNotFoundError(
            f"Файл {AUTOTESTS_FILE} не найден. Поместите его в папку {RESOURCES_DIR}."
        )

    autotests_df = pd.read_excel(AUTOTESTS_FILE)

    start_params_df = pd.DataFrame(
        columns=[
            "Порядковый номер",
            "ID мероприятия",
            "Параметры мероприятия",
            "Дата проведения",
            "Название",
            "Статус",
        ]
    )

    order_number = 1

    for _, row in autotests_df.iterrows():
        try:
            events = process_events(row["Мероприятие"])
            years = process_years(row["Год"])

            for event in events:
                for year in years:
                    event_id, event_params = event
                    new_row_df = pd.DataFrame(
                        {
                            "Порядковый номер": order_number,
                            "ID мероприятия": event_id,
                            "Название": ["Для заполнения вручную"],
                            "Дата проведения": [datetime(year, 1, 1)],
                            "Параметры мероприятия": event_params,
                            "Статус": [True],
                        }
                    )
                    if not new_row_df.empty:
                        start_params_df = pd.concat(
                            [start_params_df, new_row_df],
                            ignore_index=True,
                        )
            order_number += 1
        except Exception as e:
            raise Exception(f"Ошибка обработки строки: {row}. Детали: {e}")

    
    
    start_params_df.to_excel(START_PARAMS_FILE, index=False)
    print(f"Файл {START_PARAMS_FILE} успешно создан.")
    
    

if __name__ == "__main__":
    try:
        run_stage_one()
    except Exception as e:
        print(f"Ошибка выполнения первого этапа: {e}")
        sys.exit(1)
