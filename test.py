import pandas as pd

# Загрузка данных
base_data = {
    "dt": ["2025-01-01", "2026-01-01", "2027-01-01", "2028-01-01", "2029-01-01", 
           "2030-01-01", "2031-01-01", "2032-01-01", "2033-01-01", "2034-01-01", "2035-01-01"],
    "sum": [66654128384, 49561658880, 38527899392, 31504357376, 26694878720,
            22963292160, 20194305536, 18026582528, 16248747008, 14737743360, 13523870208],
    "experiments": [0] * 11,
    "analytics": ["Годовая добыча"] * 11,
    "header_uuids": ["dfa647c6-8fee-4bb9-a41f-843da024c3de"] * 11,
    "execution_uuid": ["5e76df9b-836a-4a4d-bd11-1ff544ae30e7"] * 11
}

experiment_data = {
    "dt": ["2025-01-01", "2026-01-01", "2027-01-01", "2028-01-01", "2029-01-01", 
           "2030-01-01", "2031-01-01", "2032-01-01", "2033-01-01", "2034-01-01", "2035-01-01"],
    "sum": [66654128384, 49561658880, 38527899392, 31504357376, 26694878720,
            28915713024, 21792686080, 19134873600, 20313145344, 14888337408, 12527337472],
    "experiments": ["5e76df9b-836a-4a4d-bd11-1ff544ae30e7"] * 11,
    "analytics": ["Годовая добыча"] * 11,
    "header_uuids": ["dfa647c6-8fee-4bb9-a41f-843da024c3de"] * 11,
    "execution_uuid": ["5e76df9b-836a-4a4d-bd11-1ff544ae30e7"] * 11
}

base_df = pd.DataFrame(base_data)
experiment_df = pd.DataFrame(experiment_data)

# Тренды
trend_info = [(2025, 0), (2026, 0), (2027, 0), (2028, 0), (2029, 0), 
              (2030, 1), (2031, -1), (2032, -1), (2033, 1), (2034, 0), (2035, -1)]

TREND_PERMISSIBLE_ERROR = 5  # Порог изменения

# Функция для расчета тренда
def calculate_trend(base_value, compare_value, n_percent):
    """Определяет тренд между двумя значениями (увеличение, уменьшение, неизменность)."""
    if abs(base_value - compare_value) / base_value * 100 <= n_percent:
        return 0  # Осталось тем же
    elif compare_value > base_value:
        return 1  # Увеличение
    else:
        return -1  # Уменьшение

# Сравнение базовой и экспериментальной таблицы
def compare_trends(base_df, experiment_df, trend_info, n_percent):
    # Преобразуем dt в год для удобства
    base_df["year"] = pd.to_datetime(base_df["dt"]).dt.year
    experiment_df["year"] = pd.to_datetime(experiment_df["dt"]).dt.year

    # Результаты
    results = []

    # Идем по трендам
    for year, expected_trend in trend_info:
        # Проверяем, есть ли год в обеих таблицах
        if year in base_df["year"].values and year in experiment_df["year"].values:
            base_value = base_df.loc[base_df["year"] == year, "sum"].values[0]
            compare_value = experiment_df.loc[experiment_df["year"] == year, "sum"].values[0]

            # Рассчитываем тренд
            actual_trend = calculate_trend(base_value, compare_value, n_percent)

            # Сравниваем тренд с ожидаемым
            results.append({
                "year": year,
                "base_sum": base_value,
                "compare_sum": compare_value,
                "expected_trend": expected_trend,
                "actual_trend": actual_trend,
                "match": actual_trend == expected_trend,
            })

    return pd.DataFrame(results)

# Сравнение
result = compare_trends(base_df, experiment_df, trend_info, TREND_PERMISSIBLE_ERROR)

# Вывод результата
print(result)
