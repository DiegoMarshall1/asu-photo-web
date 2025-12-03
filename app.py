import io
import re
import datetime as dt

from flask import Flask, render_template, request, send_file, redirect, url_for, flash
import pandas as pd
import requests


app = Flask(__name__)
app.secret_key = "asu_photo_secret_key"  # для flash-сообщений

# простой кэш, чтобы не дергать один и тот же URL много раз
_url_datetime_cache: dict[str, dt.datetime | None] = {}


def extract_datetime_from_string(s: str):
    """Ищет в строке шаблон saved-YYYYMMDD_HHMMSS и возвращает datetime или None."""
    if not isinstance(s, str):
        return None
    match = re.search(r"saved-(\d{8})_(\d{6})", s)
    if not match:
        return None
    date_part = match.group(1)  # YYYYMMDD
    time_part = match.group(2)  # HHMMSS
    try:
        year = int(date_part[0:4])
        month = int(date_part[4:6])
        day = int(date_part[6:8])
        hour = int(time_part[0:2])
        minute = int(time_part[2:4])
        second = int(time_part[4:6])
        return dt.datetime(year, month, day, hour, minute, second)
    except ValueError:
        return None


def get_datetime_from_url(raw_url: str):
    """
    Принимает URL из Excel (вида .../get-photo-url/45964825/),
    делает HTTP-запрос, берёт финальный URL с saved-YYYYMMDD_HHMMSS
    и вытаскивает дату/время.
    """
    if not isinstance(raw_url, str) or not raw_url.strip():
        return None

    # иногда ссылку копируют с ведущим '@'
    url = raw_url.strip().lstrip("@")

    if url in _url_datetime_cache:
        return _url_datetime_cache[url]

    try:
        resp = requests.get(url, allow_redirects=True, timeout=15)
        final_url = resp.url
        dt_obj = extract_datetime_from_string(final_url)
    except Exception:
        dt_obj = None

    _url_datetime_cache[url] = dt_obj
    return dt_obj


def format_interval(minutes: int):
    """minutes -> строка '45 мин' или '1 ч 25 мин' или 'ошибка'."""
    if minutes is None:
        return ""
    if minutes < 0:
        return "ошибка"
    if minutes < 60:
        return f"{minutes} мин"
    hours = minutes // 60
    mins = minutes % 60
    if mins == 0:
        return f"{hours} ч"
    return f"{hours} ч {mins} мин"


def detect_photo_columns(df: pd.DataFrame):
    """Находит два столбца с фото.

    1) Сначала пробуем по точным названиям 'Фото ДО' и 'Фото ПОСЛЕ' (как в вашем файле).
    2) Если не нашли — резервный вариант: любые два текстовых столбца с 'saved-' в значениях.
    """
    # 1. По заголовкам
    cols_lower = {str(c).strip().lower(): c for c in df.columns}
    before_col = cols_lower.get("фото до")
    after_col = cols_lower.get("фото после")
    if before_col is not None and after_col is not None:
        return before_col, after_col

    # 2. Старый вариант — по содержимому saved-
    candidate_cols = []
    for col in df.columns:
        series = df[col]
        if not pd.api.types.is_object_dtype(series):
            continue
        contains_saved = series.astype(str).str.contains("saved-", na=False)
        if contains_saved.any():
            candidate_cols.append(col)
        if len(candidate_cols) >= 2:
            break
    if len(candidate_cols) < 2:
        return None, None
    return candidate_cols[0], candidate_cols[1]


def process_dataframe(df: pd.DataFrame):
    """Обрабатывает DataFrame и возвращает (df_processed, before_col, after_col, preview_html)."""
    before_col, after_col = detect_photo_columns(df)
    if before_col is None or after_col is None:
        raise ValueError("Не найдены два столбца со ссылками на фото (столбцы 'Фото ДО' и 'Фото ПОСЛЕ').")

    df = df.copy()

    before_dates = []
    after_dates = []
    interval_minutes_raw = []
    interval_strs = []

    for _, row in df.iterrows():
        before_val = row.get(before_col)
        after_val = row.get(after_col)

        dt_before = get_datetime_from_url(before_val)
        dt_after = get_datetime_from_url(after_val)

        before_dates.append(dt_before)
        after_dates.append(dt_after)

        if dt_before is None or dt_after is None:
            interval_minutes_raw.append(None)
            interval_strs.append("")
        else:
            delta = dt_after - dt_before
            minutes = int(delta.total_seconds() // 60)
            if minutes < 0:
                interval_minutes_raw.append(None)
                interval_strs.append("ошибка")
            else:
                interval_minutes_raw.append(minutes)
                interval_strs.append(format_interval(minutes))

    df["Дата_время_до"] = before_dates
    df["Дата_время_после"] = after_dates
    df["Интервал_мин"] = interval_strs
    df["interval_minutes_raw"] = interval_minutes_raw

    # Предпросмотр
    cols_to_show = [
        before_col,
        after_col,
        "Дата_время_до",
        "Дата_время_после",
        "Интервал_мин",
    ]
    preview_df = df[cols_to_show + ["interval_minutes_raw"]].head(15).copy()

    # Форматируем даты для предпросмотра
    for col in ["Дата_время_до", "Дата_время_после"]:
        preview_df[col] = preview_df[col].apply(
            lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if isinstance(x, dt.datetime) else ""
        )

    # Отрисуем HTML-таблицу без служебной колонки
    interval_list = preview_df["interval_minutes_raw"].tolist()
    preview_df = preview_df.drop(columns=["interval_minutes_raw"])
    preview_html = preview_df.to_html(
        classes="table table-striped table-bordered table-sm",
        index=False,
        border=0,
        escape=False,
    )

    return df, before_col, after_col, preview_html, interval_list


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "GET":
        return render_template("index.html", preview_html=None, download_url=None)

    # POST: загрузка и обработка файла
    uploaded = request.files.get("file")
    if not uploaded or uploaded.filename == "":
        flash("Пожалуйста, выберите Excel-файл.")
        return redirect(url_for("index"))

    filename = uploaded.filename
    if not (filename.lower().endswith(".xlsx") or filename.lower().endswith(".xls")):
        flash("Поддерживаются только файлы .xlsx и .xls")
        return redirect(url_for("index"))

    try:
        # Читаем Excel из памяти
        file_bytes = uploaded.read()
        buffer = io.BytesIO(file_bytes)

        if filename.lower().endswith(".xlsx"):
            df = pd.read_excel(buffer, engine="openpyxl")
        else:
            df = pd.read_excel(buffer)

        df_processed, before_col, after_col, preview_html, _intervals = process_dataframe(
            df
        )
    except Exception as exc:
        flash(f"Ошибка обработки файла: {exc}")
        return redirect(url_for("index"))

    # Сохраняем обработанный файл в сессию как байты (в реальном проде — в хранилище)
    output = io.BytesIO()
    df_to_save = df_processed.copy()
    if "interval_minutes_raw" in df_to_save.columns:
        df_to_save = df_to_save.drop(columns=["interval_minutes_raw"])
    df_to_save.to_excel(output, index=False, engine="openpyxl")
    output.seek(0)

    # Кладём в глобальный dict по простому ключу (один пользователь за раз)
    app.config["LAST_PROCESSED_FILE"] = output
    app.config["LAST_PROCESSED_NAME"] = filename.rsplit(".", 1)[0] + "_обработанный.xlsx"

    flash(f"Файл успешно обработан. Найдены столбцы: '{before_col}' (до), '{after_col}' (после).")
    return render_template(
        "index.html",
        preview_html=preview_html,
        download_url=url_for("download_result"),
    )


@app.route("/download")
def download_result():
    output = app.config.get("LAST_PROCESSED_FILE")
    name = app.config.get("LAST_PROCESSED_NAME", "result.xlsx")
    if output is None:
        flash("Нет обработанного файла. Сначала загрузите и обработайте Excel.")
        return redirect(url_for("index"))
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name=name,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    # Локальный запуск: http://127.0.0.1:5000
    app.run(host="0.0.0.0", port=5000, debug=True)


