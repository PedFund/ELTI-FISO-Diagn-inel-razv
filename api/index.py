"""
Serverless Flask API for processing diagnostic Excel data on Vercel.

This module defines a small Flask application that accepts Excel files,
normalises and calculates a number of derived metrics, and returns a new
Excel workbook containing multiple sheets and charts.  It is intended to
be deployed as a Vercel serverless function, so the file lives inside
an `api` directory and is referenced by `vercel.json`.

The code imports all of its runtime dependencies at the top level so
Vercel knows to include them in the deployed package.  If a dependency
is missing at runtime, the application will respond with a JSON error
instead of an HTML page, which makes error handling in the client
simpler.
"""

from flask import Flask, request, send_file, jsonify, render_template_string
import pandas as pd
# The following imports are intentionally unused directly in the code but
# required to ensure that Vercel bundles these optional engines.  Pandas
# chooses the Excel writer engine automatically if it is available.  If
# `openpyxl` or `xlsxwriter` are missing, pandas will raise an
# ImportError when attempting to write files.  By importing them here
# explicitly, they are included in the deployment bundle.
import openpyxl  # noqa: F401  pylint: disable=unused-import
import xlsxwriter  # noqa: F401  pylint: disable=unused-import
import os
import re
import io
from werkzeug.utils import secure_filename  # noqa: F401

# -----------------------------------------------------------------------------
# Flask application setup
# -----------------------------------------------------------------------------

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MB upload limit

# Column mapping from raw column names to normalised names.  See project
# documentation for details on the meaning of each field.
COLUMN_MAPPING = {
    "ID": "ID",
    "Время создания": "Время",
    "Наименование вашей образовательной организации": "Организация",
    "Код ребёнка": "Код",
    "Согласие родителя (законного представителя) на проведение психолого-педагогического обследования и сопровождение ребёнка получено": "Согласие_обслед",
    "Согласие родителя (законного представителя) на обработку персональных данных ребёнка получено": "Согласие_ПД",
    "Возрастная группа, к которой относится ребенок в текущем учебном году": "Возраст",
    "Ввести возрастную группу": "Возраст_ввод",
    'Субтест И1-1 "Рассуждения". Укажите суммарную оценку': "И1-1Сум",
    'Субтест И1-2 "Рассуждения". Укажите значение по каждому критерию / Связность': "И1-2Связн",
    'Субтест И1-2 "Рассуждения". Укажите значение по каждому критерию / Речевое оформление высказываний': "И1-2РечОформ",
    'Субтест И1-2 "Рассуждения". Укажите значение по каждому критерию / Самостоятельность рассуждения': "И1-2СамРасс",
    'Субтест И2 "Сходство". Укажите суммарную оценку.': "И2Сум",
    'Субтест И3-1-1 "Будь внимателен". Введите количество колец, просмотренных за 1-ю минуту (целое число)': "И3-1Кольца",
    'Субтест И3-1-2 "Будь внимателен". Введите количество ошибок, допущенных за 1-ю минуту (целое число)': "И3-1Ошиб",
    'Субтест И3-2-1 "Будь внимателен". Введите количество колец, просмотренных за 2-ю минуту (целое число)': "И3-2Кольца",
    'Субтест И3-2-2 "Будь внимателен". Введите количество ошибок, допущенных за 2-ю минуту (целое число)': "И3-2Ошиб",
    'Субтест И3-3-1 "Будь внимателен". Введите количество колец, просмотренных за 3-ю минуту (целое число)': "И3-3Кольца",
    'Субтест И3-3-2 "Будь внимателен". Введите количество ошибок, допущенных за 3-ю минуту (целое число)': "И3-3Ошиб",
    'Субтест И3-4-1 "Будь внимателен". Введите количество колец, просмотренных за 4-ю минуту (целое число)': "И3-4Кольца",
    'Субтест И3-4-2 "Будь внимателен". Введите количество ошибок, допущенных за 4-ю минуту (целое число)': "И3-4Ошиб",
    'Субтест И3-5-1 "Будь внимателен". Введите количество колец, просмотренных за 5-ю минуту (целое число)': "И3-5Кольца",
    'Субтест И3-5-2 "Будь внимателен". Введите количество ошибок, допущенных за 5-ю минуту (целое число)': "И3-5Ошиб",
    'Субтест И4 "Недостающие детали". Введите число верных ответов': "И4Сум",
    'Субтест И5-1-1 "Лабиринты": укажите время прохождения лабиринта 1 (в секундах)': "И5-1Время",
    'Субтест И5-1-2 "Лабиринты": укажите количество ошибок при прохождении лабиринта 1 (целое число)': "И5-1Ошиб",
    'Субтест И5-1-3 "Лабиринты". Отметьте, дошел ли ребенок до цели в указанное Вами время в лабиринте 1': "И5-1Дошел",
    'Субтест И5-2-1 "Лабиринты": укажите время прохождения лабиринта 2 (в секундах)': "И5-2Время",
    'Субтест И5-2-2 "Лабиринты": укажите количество ошибок при прохождении лабиринта 2 (целое число)': "И5-2Ошиб",
    'Субтест И5-2-3 "Лабиринты". Отметьте, дошел ли ребенок до цели в указанное Вами время в лабиринте 2': "И5-2Дошел",
    'Субтест И5-3-1 "Лабиринты": укажите время прохождения лабиринта 3 (в секундах)': "И5-3Время",
    'Субтест И5-3-2 "Лабиринты": укажите количество ошибок при прохождении лабиринта 3 (целое число)': "И5-3Ошиб",
    'Субтест И5-3-3 "Лабиринты". Отметьте, дошел ли ребенок до цели в указанное Вами время в лабиринте 3': "И5-3Дошел",
    'Субтест И5-4-1 "Лабиринты": укажите время прохождения лабиринта 4 (в секундах)': "И5-4Время",
    'Субтест И5-4-2 "Лабиринты": укажите количество ошибок при прохождении лабиринта 4 (целое число)': "И5-4Ошиб",
    'Субтест И5-4-3 "Лабиринты". Отметьте, дошел ли ребенок до цели в указанное Вами время в лабиринте 4': "И5-4Дошел",
    'Субтест И5-5-1 "Лабиринты": укажите время прохождения лабиринта 5 (в секундах)': "И5-5Время",
    'Субтест И5-5-2 "Лабиринты": укажите количество ошибок при прохождении лабиринта 5 (целое число)': "И5-5Ошиб",
    'Субтест И5-5-3 "Лабиринты". Отметьте, дошел ли ребенок до цели в указанное Вами время в лабиринте 5': "И5-5Дошел",
    'Адаптированная проба "Художник". \nВведите значения по шкалам В1 и В2. / В1': "В1",
    'Адаптированная проба "Художник". \nВведите значения по шкалам В1 и В2. / В2': "В2",
    'Адаптированная проба "Художник". Для экспертного определения значения по шкале В3 присоедините качественное фото или скан рисунка размером до 1 Мб.': "В3_фото",
    'Методика идентификации базовых эмоций. / Укажите итоговую оценку': "ЭмоцИдент",
    'Методика наблюдения за совместной деятельностью. Укажите средние значения результатов экспертного наблюдения по видам деятельности / Планирование': "Планир",
    'Методика наблюдения за совместной деятельностью. Укажите средние значения результатов экспертного наблюдения по видам деятельности / Сотрудничество и сотворчество': "Сотруд",
    'Методика наблюдения за совместной деятельностью. Укажите средние значения результатов экспертного наблюдения по видам деятельности / Рефлексия': "Рефлек",
    'При необходимости ниже Вы можете указать свои примечания, либо оставить данное поле пустым.': "Примеч"
}


def calc_lab(time, errors, reached, limit):
    """Calculate a labyrinth score based on time, error count and whether goal was reached."""
    try:
        time = float(time)
    except Exception:
        return 0
    try:
        errors = int(errors)
    except Exception:
        errors = 0
    # If the child did not reach the goal explicitly mark as zero
    if isinstance(reached, str) and reached.strip() == "Нет":
        return 0
    if time > limit:
        return 0
    if errors == 0:
        return 3
    if errors == 1:
        return 2
    if 2 <= errors <= 5:
        return 1
    return 0


def attention_index(rings, errors):
    """Return a numeric index of attentional quality based on counts and errors."""
    try:
        rings = float(rings)
    except Exception:
        rings = 0
    try:
        errors = float(errors)
    except Exception:
        errors = 0
    # According to methodology: 0.5*rings - (2.8*errors)/60
    return 0.5 * rings - (2.8 * errors) / 60


def categorize(value):
    """Map a continuous value onto a qualitative level description."""
    if pd.isna(value):
        return None
    if value < 0.33:
        return "ниже нормативного"
    elif value <= 0.66:
        return "нормативный"
    else:
        return "выше нормативного"


def extract_town(org_name):
    """Extract the town name from the organisation name in parentheses."""
    match = re.search(r"\((.*?)\)", str(org_name))
    if match:
        return match.group(1).split(";")[0].strip()
    return None


def sort_key_town(name):
    """Return a sortable tuple to order Russian town names in a meaningful way."""
    if pd.isna(name):
        return (999, "")
    name = str(name).strip()
    if name == "г.Москва":
        return (0, name)
    if name.startswith("г."):
        return (1, name)
    if name.startswith(("р.п.", "п.", "пос.")):
        return (2, name)
    if name.startswith("с."):
        return (3, name)
    if name.startswith("д."):
        return (4, name)
    if name.startswith("ст."):
        return (5, name)
    return (6, name)


def process_excel(file_content: bytes, filename: str):
    """
    Main data processing routine.

    Takes the raw bytes of an uploaded Excel file and its filename,
    validates the name, cleans and normalises the data, calculates
    additional metrics, builds intermediate tables and finally
    constructs a multi-sheet Excel workbook in memory.  The workbook
    includes charts for age distribution, medians and level counts.

    :param file_content: Raw bytes of the uploaded Excel file.
    :param filename: Name of the uploaded file.
    :return: Tuple (binary Excel content, suggested filename).
    :raises ValueError: If the filename does not match the expected pattern.
    """
    # Validate filename (e.g. "5-31-Razvitie.xlsx")
    match = re.match(r'(\d+)-(\d+)', filename)
    if not match:
        raise ValueError("Неверный формат имени файла. Ожидается: {площадка}-{диагностика}-*.xlsx")
    ploshchadka = match.group(1)
    diagnostika = match.group(2)

    # Read the first sheet of the Excel file
    df = pd.read_excel(io.BytesIO(file_content), sheet_name=0)
    # Normalise column names
    df = df.rename(columns=COLUMN_MAPPING)

    # Derive labyrinth scores (П1–П5) using the appropriate time limits
    df["П1"] = df.apply(lambda x: calc_lab(x.get("И5-1Время"), x.get("И5-1Ошиб"), x.get("И5-1Дошел"), 35), axis=1)
    df["П2"] = df.apply(lambda x: calc_lab(x.get("И5-2Время"), x.get("И5-2Ошиб"), x.get("И5-2Дошел"), 35), axis=1)
    df["П3"] = df.apply(lambda x: calc_lab(x.get("И5-3Время"), x.get("И5-3Ошиб"), x.get("И5-3Дошел"), 50), axis=1)
    df["П4"] = df.apply(lambda x: calc_lab(x.get("И5-4Время"), x.get("И5-4Ошиб"), x.get("И5-4Дошел"), 65), axis=1)
    df["П5"] = df.apply(lambda x: calc_lab(x.get("И5-5Время"), x.get("И5-5Ошиб"), x.get("И5-5Дошел"), 125), axis=1)
    # Average labyrinth score mapped onto a 0–1 scale
    df["Аналит-Синт"] = ((df[["П1", "П2", "П3", "П4", "П5"]].mean(axis=1)) / 3).round(2)

    # Attention indices for each minute and their mean
    df["Вним1"] = df.apply(lambda x: attention_index(x.get("И3-1Кольца"), x.get("И3-1Ошиб")), axis=1)
    df["Вним2"] = df.apply(lambda x: attention_index(x.get("И3-2Кольца"), x.get("И3-2Ошиб")), axis=1)
    df["Вним3"] = df.apply(lambda x: attention_index(x.get("И3-3Кольца"), x.get("И3-3Ошиб")), axis=1)
    df["Вним4"] = df.apply(lambda x: attention_index(x.get("И3-4Кольца"), x.get("И3-4Ошиб")), axis=1)
    df["Вним5"] = df.apply(lambda x: attention_index(x.get("И3-5Кольца"), x.get("И3-5Ошиб")), axis=1)
    df["СредВним"] = df[["Вним1", "Вним2", "Вним3", "Вним4", "Вним5"]].mean(axis=1)
    df["Качество внимания"] = df["СредВним"].apply(lambda v: 1 if v >= 6 else round(v / 6, 2))

    # Normalise criterion scores to a 0–1 scale
    df["Связн"] = (df["И1-2Связн"] / 5).round(2)
    df["РечОформ"] = (df["И1-2РечОформ"] / 5).round(2)
    df["СамостРасс"] = (df["И1-2СамРасс"] / 5).round(2)

    # Derived composite metrics
    df["Готовн_УД"] = ((df["И1-1Сум"] / 18 + (df["Связн"] + df["РечОформ"] + df["СамостРасс"]) / 3) / 2).round(2)
    df["Лог_обобщение"] = (df["И2Сум"] / 16).round(2)
    df["Перцепция"] = (df["И4Сум"] / 11).round(2)
    df["Активн_вниман"] = df["Качество внимания"]
    df["Аналит_синт"] = df["Аналит-Синт"]
    df["Воображение"] = (((df["В1"] / 3) + (df["В2"] / 3)) / 2).round(2)
    df["Идентиф_эмоций"] = (df["ЭмоцИдент"] / 8).round(2)
    df["Планирование"] = (df["Планир"] / 4).round(2)
    df["Сотрудничество"] = (df["Сотруд"] / 4).round(2)
    df["Рефлексия"] = (df["Рефлек"] / 4).round(2)
    df["Когнитивное развитие"] = ((df["Готовн_УД"] + df["Активн_вниман"] + df["Аналит_синт"] + df["Лог_обобщение"] + df["Перцепция"]) / 5).round(2)
    df["Воображение_итог"] = df["Воображение"]
    df["ЭмСоцИнтеллект"] = ((df["Идентиф_эмоций"] + (df["Планирование"] + df["Сотрудничество"] + df["Рефлексия"]) / 3) / 2).round(2)

    # Qualitative level descriptors
    for col in ["Когнитивное развитие", "Воображение_итог", "ЭмСоцИнтеллект"]:
        df[col + "_уровень"] = df[col].apply(categorize)

    # Prepare summary tables
    level_tables = {}
    total = len(df)
    for metr in ["Когнитивное развитие", "Воображение_итог", "ЭмСоцИнтеллект"]:
        t = df[metr + "_уровень"].value_counts().reindex(
            ["ниже нормативного", "нормативный", "выше нормативного"], fill_value=0
        ).reset_index()
        t.columns = ["Уровень", "Количество детей"]
        t["Доля"] = t["Количество детей"] / total
        level_tables[metr] = t

    cols_for_median = [
        "Когнитивное развитие", "Готовн_УД", "Лог_обобщение", "Перцепция",
        "Качество внимания", "Аналит-Синт", "Связн", "РечОформ", "СамостРасс",
        "Воображение", "ЭмСоцИнтеллект", "Идентиф_эмоций",
        "Планирование", "Сотрудничество", "Рефлексия"
    ]
    median_table = df[cols_for_median].median().round(2).reset_index()
    median_table.columns = ["Показатель", "Медианное значение"]

    age_counts = df["Возраст"].value_counts().reset_index()
    age_counts.columns = ["Возрастная группа", "Количество детей"]
    age_counts["Количество детей в %"] = (age_counts["Количество детей"] / age_counts["Количество детей"].sum())
    # Attempt to sort age groups numerically when possible
    def extract_age(text):
        try:
            return int(str(text).split("-")[0].split()[0])
        except Exception:
            return 999
    age_counts["age_sort"] = age_counts["Возрастная группа"].apply(extract_age)
    age_counts = age_counts.sort_values("age_sort").drop(columns="age_sort").reset_index(drop=True)

    df["Населённый пункт"] = df["Организация"].apply(extract_town)
    towns = df.groupby("Населённый пункт")["Организация"].nunique().reset_index()
    towns.columns = ["Населённый пункт", "Количество организаций"]
    towns = towns[towns["Населённый пункт"].notna()]
    towns = towns[towns["Количество организаций"] > 0]
    towns = towns.sort_values(by="Населённый пункт", key=lambda col: col.map(sort_key_town))
    total_row = pd.DataFrame({
        "Населённый пункт": ["Итого"],
        "Количество организаций": [towns["Количество организаций"].sum()]
    })
    towns_with_total = pd.concat([towns, total_row], ignore_index=True)

    # Export normalised per-child metrics
    df_export = pd.DataFrame({
        "Код": df["Код"],
        "Возраст": df["Возраст"],
        "Связность": df["Связн"],
        "Речевое оформление": df["РечОформ"],
        "Самостоятельность рассуждения": df["СамостРасс"],
        "Аналит-Синт": df["Аналит-Синт"],
        "Качество внимания": df["Качество внимания"],
        "Готовн_УД": df["Готовн_УД"],
        "Лог_обобщение": df["Лог_обобщение"],
        "Перцепция": df["Перцепция"],
        "Активн_вниман": df["Активн_вниман"],
        "Аналит_синт": df["Аналит_синт"],
        "Воображение": df["Воображение"],
        "Идентиф_эмоций": df["Идентиф_эмоций"],
        "Планирование": df["Планирование"],
        "Сотрудничество": df["Сотрудничество"],
        "Рефлексия": df["Рефлексия"],
        "Когнитивное развитие": df["Когнитивное развитие"],
        "ЭмСоцИнтеллект": df["ЭмСоцИнтеллект"],
        "Когнитивное развитие_уровень": df["Когнитивное развитие_уровень"],
        # Rename "Воображение_итог_уровень" to user-friendly column name
        "Воображение_уровень": df["Воображение_итог_уровень"],
        "ЭмСоцИнтеллект_уровень": df["ЭмСоцИнтеллект_уровень"],
    })

    # Build the Excel workbook in memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        wb = writer.book

        # Define some common formats
        fmt_header = wb.add_format({"bold": True, "bg_color": "#D9E1F2", "align": "center", "valign": "vcenter", "border": 1})
        fmt_num = wb.add_format({"num_format": "0.00", "align": "center"})
        fmt_pct = wb.add_format({"num_format": "0.0%", "align": "center"})
        fmt_text = wb.add_format({"align": "left"})
        bold_fmt = wb.add_format({"bold": True, "bg_color": "#D9E1F2", "align": "center", "valign": "vcenter", "border": 1})

        # Sheet 1: Age distribution with pie chart
        age_counts.to_excel(writer, sheet_name="Возрастные группы", index=False)
        ws_age = writer.sheets["Возрастные группы"]
        ch_age = wb.add_chart({"type": "pie"})
        ch_age.add_series({
            "name": "Возрастные группы",
            "categories": ["Возрастные группы", 1, 0, len(age_counts), 0],
            "values": ["Возрастные группы", 1, 1, len(age_counts), 1],
            "data_labels": {"percentage": True, "category": True}
        })
        ch_age.set_title({"name": "Распределение детей по возрастным группам"})
        ws_age.insert_chart("E2", ch_age, {"x_scale": 1.3, "y_scale": 1.3})

        # Sheet 2: Medians with radar chart
        median_table.to_excel(writer, sheet_name="Медианы", index=False)
        ws_median = writer.sheets["Медианы"]
        ch_med = wb.add_chart({"type": "radar"})
        ch_med.add_series({
            "name": "Медианные значения",
            "categories": ["Медианы", 1, 0, len(median_table), 0],
            "values": ["Медианы", 1, 1, len(median_table), 1],
            "marker": {"type": "circle", "size": 5},
            "line": {"color": "#0070C0"},
        })
        ch_med.set_title({"name": "Медианные значения показателей"})
        ws_median.insert_chart("E2", ch_med, {"x_scale": 1.5, "y_scale": 1.5})

        # Sheets 3–5: Level distribution for each composite metric
        for metr in ["Когнитивное развитие", "Воображение_итог", "ЭмСоцИнтеллект"]:
            sh = metr.replace(" ", "_")[:30]
            table = level_tables[metr]
            table.to_excel(writer, sheet_name=sh, index=False)
            ws = writer.sheets[sh]
            # Apply formatting on header
            for c, name in enumerate(table.columns):
                ws.write(0, c, name, fmt_header)
            ws.set_column(0, 0, 20, fmt_text)
            ws.set_column(1, 1, 12, fmt_num)
            ws.set_column(2, 2, 12, fmt_pct)
            ch = wb.add_chart({"type": "column"})
            ch.add_series({
                "name": metr,
                "categories": [sh, 1, 0, 3, 0],
                "values": [sh, 1, 2, 3, 2],
                "data_labels": {"value": True}
            })
            ch.set_title({"name": f"Распределение уровней (%): {metr}"})
            ch.set_y_axis({"num_format": "0%"})
            ch.set_legend({"position": "bottom"})
            ws.insert_chart("E2", ch, {"x_scale": 1.3, "y_scale": 1.3})

        # Sheet 6: Town summary with total row
        towns_with_total.to_excel(writer, sheet_name="Населённые пункты", index=False)
        ws_towns = writer.sheets["Населённые пункты"]
        ws_towns.set_row(len(towns_with_total), None, bold_fmt)

        # Sheet 7: Normalised per-child metrics
        df_export.to_excel(writer, sheet_name="Нормированные_показатели", index=False)
        ws_norm = writer.sheets["Нормированные_показатели"]
        for c, name in enumerate(df_export.columns):
            ws_norm.write(0, c, name, fmt_header)
            if name in ["Код", "Возраст", "Когнитивное развитие_уровень", "Воображение_уровень", "ЭмСоцИнтеллект_уровень"]:
                ws_norm.set_column(c, c, 18, fmt_text)
            else:
                ws_norm.set_column(c, c, 12, fmt_num)

    output.seek(0)
    output_filename = f"Аналитика_{ploshchadka}-{diagnostika}.xlsx"
    return output.getvalue(), output_filename


@app.route('/')
def index():
    """Serve the minimal HTML upload interface."""
    html = '''<!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Обработка диагностики</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                padding: 20px;
            }
            .container {
                max-width: 700px;
                margin: 0 auto;
                background: white;
                border-radius: 20px;
                padding: 40px;
                box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            }
            h1 { color: #667eea; text-align: center; margin-bottom: 30px; }
            .upload-zone {
                border: 3px dashed #667eea;
                border-radius: 15px;
                padding: 60px 30px;
                text-align: center;
                cursor: pointer;
                transition: all 0.3s;
            }
            .upload-zone:hover { background: #f8f9ff; border-color: #764ba2; }
            input[type="file"] { display: none; }
            .btn {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                border: none;
                padding: 15px 40px;
                border-radius: 10px;
                font-size: 16px;
                font-weight: 600;
                cursor: pointer;
                margin-top: 20px;
            }
            .btn:hover { transform: translateY(-2px); box-shadow: 0 10px 20px rgba(102, 126, 234, 0.4); }
            .status {
                margin-top: 30px;
                padding: 20px;
                border-radius: 10px;
                display: none;
            }
            .status.success { background: #d4edda; color: #155724; }
            .status.error { background: #f8d7da; color: #721c24; }
            .status.processing { background: #d1ecf1; color: #0c5460; }
            .info {
                margin-top: 30px;
                padding: 20px;
                background: #f8f9ff;
                border-radius: 10px;
                font-size: 14px;
            }
            code { background: #f0f0f0; padding: 2px 6px; border-radius: 4px; color: #d63384; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 Обработка диагностики</h1>
            <form id="uploadForm" enctype="multipart/form-data">
                <div class="upload-zone" id="uploadZone">
                    <h2>📁 Загрузите Excel файл</h2>
                    <p>Формат: <code>{площадка}-{диагностика}-*.xlsx</code></p>
                    <input type="file" id="fileInput" name="file" accept=".xlsx,.xls" required>
                    <button type="button" class="btn" onclick="document.getElementById('fileInput').click()">
                        Выбрать файл
                    </button>
                    <!-- Контейнер для отображения выбранного имени файла -->
                    <p id="fileName" style="margin-top:10px; color:#333;"></p>
                </div>
                <div style="text-align: center; margin-top: 20px;">
                    <button type="submit" class="btn">Обработать</button>
                </div>
            </form>
            <div class="status" id="status"></div>
            <div class="info">
                <h3>ℹ️ Информация:</h3>
                <ul>
                    <li>Имя файла: <code>{площадка}-{диагностика}-*.xlsx</code></li>
                    <li>Площадка: 1-20, Диагностика: 31, 41, 42</li>
                    <li>Результат: 7 листов Excel + графики</li>
                </ul>
            </div>
        </div>
        <script>
            // Отображаем выбранное имя файла
            const fileInput = document.getElementById('fileInput');
            const fileNameLabel = document.getElementById('fileName');
            fileInput.addEventListener('change', () => {
                if (fileInput.files && fileInput.files.length > 0) {
                    fileNameLabel.textContent = 'Выбран файл: ' + fileInput.files[0].name;
                } else {
                    fileNameLabel.textContent = '';
                }
            });

            document.getElementById('uploadForm').addEventListener('submit', async (e) => {
                e.preventDefault();
                const formData = new FormData();
                const status = document.getElementById('status');
                if (!fileInput.files || !fileInput.files[0]) {
                    status.textContent = '❌ Выберите файл';
                    status.className = 'status error';
                    status.style.display = 'block';
                    return;
                }
                formData.append('file', fileInput.files[0]);
                status.textContent = '⏳ Обработка...';
                status.className = 'status processing';
                status.style.display = 'block';
                try {
                    const response = await fetch('/api/process', {
                        method: 'POST',
                        body: formData
                    });
                    if (response.ok) {
                        const blob = await response.blob();
                        const url = window.URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = url;
                        a.download = response.headers.get('X-Filename') || 'result.xlsx';
                        a.click();
                        status.textContent = '✅ Готово! Файл скачан.';
                        status.className = 'status success';
                    } else {
                        // Читаем тело ответа один раз и пытаемся распарсить его как JSON.
                        const text = await response.text();
                        let message = text || 'Неизвестная ошибка';
                        try {
                            const errObj = JSON.parse(text);
                            if (errObj && errObj.error) {
                                message = errObj.error;
                            }
                        } catch (err) {
                            // ignore JSON parse errors
                        }
                        status.textContent = '❌ ' + message;
                        status.className = 'status error';
                    }
                } catch (error) {
                    status.textContent = '❌ Ошибка: ' + error.message;
                    status.className = 'status error';
                }
            });
        </script>
    </body>
    </html>'''
    return render_template_string(html)


@app.route('/api/process', methods=['POST'])
def process():
    """Handle the file upload, process it and return a new Excel file."""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'Файл не найден'}), 400
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'Файл не выбран'}), 400
        file_content = file.read()
        result_content, result_filename = process_excel(file_content, file.filename)
        response = send_file(
            io.BytesIO(result_content),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=result_filename
        )
        # Expose filename in custom header for the browser to use
        response.headers['X-Filename'] = result_filename
        return response
    except Exception as e:
        # Return errors as JSON; Vercel will otherwise wrap exceptions in an HTML page.
        return jsonify({'error': str(e)}), 500


# Bind the app instance for Vercel.  Vercel looks for a top-level variable
# called `app` when using the @vercel/python runtime.
app = app
