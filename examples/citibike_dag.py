"""
DAG для инкрементальной загрузки данных Citi Bike из S3-источника в целевой S3 бакет.

Основные отличия от исходного варианта:
- исправлена работа с execution_date в get_target_month;
- расписание переведено на совместимое с Airflow cron-выражение;
- проверка наличия архивов в бакете-источнике учитывает возможные префиксы;
- комментарии и логирование приведены к более читаемому виду.
"""

from __future__ import annotations

import logging
import os
import shutil
import zipfile
from typing import Iterable

import pendulum
from airflow.decorators import dag, task
from airflow.decorators import get_current_context
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator

SHARED_BASE_DIR = "/opt/airflow/shared_workers_data"
SOURCE_NAME = "citibike_data"
LAYER = "raw"
BUCKET_NAME_SOURCE = "citibike-data"
BUCKET_NAME_TARGET = "rzvde-g4-tomskaya-alina"
AWS_CONN_SRC = "g4__tomskaya.alina__s3_source_conn"
AWS_CONN_TRG = "g4__tomskaya.alina__s3_datalake_conn"
DATALAKE_CONN_ID = "g4__tomskaya.alina__s3_datalake_conn"
SOURCE_CONN_ID = "g4__tomskaya.alina__s3_source_conn"
my_tags = ["group:g4", "owner:tomskaya.alina", "stage:s1"]


def _find_source_key(objects: Iterable[str], filename: str) -> str | None:
    """Возвращает ключ из списка S3, который оканчивается на filename."""
    for obj in objects:
        if obj.split("/")[-1] == filename:
            return obj
    return None


@task
def create_temp_directory() -> str:
    """Создает временную директорию для сохранения файлов во время выполнения."""
    context = get_current_context()
    dag_run_id = context.get("run_id", os.environ.get("AIRFLOW_DAG_RUN_ID", "manual")).replace(":", "_")
    temp_dir = os.path.join(SHARED_BASE_DIR, f"citibike_{dag_run_id}")

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        logging.info("Очищена старая временная директория: %s", temp_dir)

    os.makedirs(temp_dir, exist_ok=True)
    logging.info("Создана временная директория: %s", temp_dir)
    return temp_dir


@task
def get_target_month(**context) -> str:
    """Определяет месяц для загрузки на основе execution_date или параметра."""
    execution_date_value = context["execution_date"]
    params = context["params"]
    yyyymm_param = params.get("YYYYMM")
    if yyyymm_param:
        yyyymm = yyyymm_param
        logging.info("Используется месяц из параметра: %s", yyyymm)
    else:
        yyyymm = execution_date_value.strftime("%Y%m")
        logging.info("Используется месяц из execution_date: %s", yyyymm)
    return yyyymm


@task
def check_if_month_already_loaded(s3_objects_target: list, yyyymm: str, **context) -> str:
    """Проверяет, загружены ли данные за указанный месяц."""
    params = context["params"]
    force_reload = params.get("force_reload", False)

    if not s3_objects_target:
        logging.info("Целевой бакет пустой. Начинаем первую загрузку.")
        return yyyymm

    month_prefix = f"{LAYER}/{SOURCE_NAME}/{yyyymm}"
    has_files = any(obj.startswith(month_prefix) for obj in s3_objects_target)

    if has_files and not force_reload:
        raise AirflowSkipException(f"Данные за {yyyymm} уже загружены. Пропускаем.")
    if has_files and force_reload:
        logging.warning("Перезагрузка данных за %s", yyyymm)
        return yyyymm

    logging.info("Начинаем загрузку данных за %s", yyyymm)
    return yyyymm


@task
def download_monthly_data(
    s3_objects_source: list, temp_dir: str, yyyymm: str, **context
) -> str:
    """Скачивает zip-файл за указанный месяц из S3 источника."""
    filename = f"{yyyymm}-citibike-tripdata.zip"
    source_key = _find_source_key(s3_objects_source, filename)
    if not source_key:
        raise FileNotFoundError(f"{filename} не найден в исходном бакете.")

    s3_hook = S3Hook(aws_conn_id=SOURCE_CONN_ID)
    local_path = os.path.join(temp_dir, filename)
    logging.info("Загрузка файла %s из S3 в %s", source_key, local_path)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    tmp_path = s3_hook.download_file(
        key=source_key,
        bucket_name=BUCKET_NAME_SOURCE,
        local_path=temp_dir,
        preserve_file_name=True,
    )

    if tmp_path and os.path.exists(tmp_path):
        shutil.move(tmp_path, local_path)

    if not os.path.exists(local_path):
        logging.error("Содержимое %s: %s", temp_dir, os.listdir(temp_dir))
        raise FileNotFoundError(f"Не удалось скачать {filename} по пути {local_path}")

    logging.info("Файл успешно загружен: %s", local_path)
    return local_path


@task
def extract_and_upload(zip_path: str, yyyymm: str, temp_dir: str, **context) -> None:
    """Распаковывает ZIP и загружает CSV файлы в целевой бакет."""
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(temp_dir)
        logging.info("Распакован ZIP: %s в %s", zip_path, temp_dir)

    s3_hook = S3Hook(aws_conn_id=DATALAKE_CONN_ID)
    csv_files = []
    for root, _, files in os.walk(temp_dir):
        for file in files:
            if file.endswith(".csv"):
                csv_files.append(os.path.join(root, file))

    if not csv_files:
        raise FileNotFoundError("Не найдены csv файлы после распаковки zip.")

    for csv_file in sorted(csv_files):
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"CSV файл отсутствует после распаковки: {csv_file}")
        filename = os.path.basename(csv_file)
        if not filename.startswith(yyyymm):
            logging.warning(
                "Пропускаем файл вне целевого месяца %s: %s", yyyymm, filename
            )
            continue
        s3_key = f"{LAYER}/{SOURCE_NAME}/{yyyymm}/{filename}"
        logging.info("Uploading %s to s3://%s/%s", csv_file, BUCKET_NAME_TARGET, s3_key)
        s3_hook.load_file(filename=csv_file, key=s3_key, bucket_name=BUCKET_NAME_TARGET, replace=True)

    logging.info("Все CSV файлы загружены в S3 за месяц %s", yyyymm)


@task
def cleanup_temp_directory(temp_dir: str, **context) -> None:
    """Удаляет временную папку."""
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        logging.info("Удалена временная папка: %s", temp_dir)


@task
def log_success(yyyymm: str) -> str:
    logging.info("🎉 ИНКРЕМЕНТАЛЬНАЯ ЗАГРУЗКА ЗАВЕРШЕНА: %s", yyyymm)
    return f"Данные за {yyyymm} успешно загружены"


@dag(
    dag_id="g4_tomskaya_alina_s1",
    start_date=pendulum.datetime(2024, 12, 11, tz="Europe/Moscow"),
    schedule_interval="0 0 1 * *",  # ежемесячно в 00:00 в первый день месяца
    catchup=True,
    tags=["citibike", "etl", "owner: tomskaya.alina", "group:g4"],
    params={
        "YYYYMM": Param(
            "",
            type="string",
            title="Месяц отгрузки",
            description="Введите месяц в формате YYYYMM (например, 202507). "
            "Если оставить пустым — возьмется execution_date.",
        ),
        "force_reload": Param(
            False,
            type="boolean",
            title="Force reload",
            description="Если true, перезагружать данные даже если они уже есть.",
        ),
    },
    default_args={"retries": 2, "retry_delay": pendulum.duration(seconds=30)},
)
def etl_pipeline():
    create_dir = create_temp_directory()
    list_source_files = S3ListOperator(
        task_id="list_s3_files_source",
        bucket=BUCKET_NAME_SOURCE,
        aws_conn_id=SOURCE_CONN_ID,
    )
    list_target_files = S3ListOperator(
        task_id="list_s3_target_files",
        bucket=BUCKET_NAME_TARGET,
        aws_conn_id=AWS_CONN_TRG,
        prefix=f"{LAYER}/{SOURCE_NAME}",
    )
    target_month = get_target_month()

    month_to_process = check_if_month_already_loaded(
        s3_objects_target=list_target_files.output, yyyymm=target_month
    )
    zip_path = download_monthly_data(
        s3_objects_source=list_source_files.output, temp_dir=create_dir, yyyymm=month_to_process
    )
    extract_and_upload_task = extract_and_upload(zip_path=zip_path, yyyymm=month_to_process, temp_dir=create_dir)
    cleanup_task = cleanup_temp_directory(temp_dir=create_dir)
    success_task = log_success(month_to_process)

    extract_and_upload_task >> success_task
    create_dir >> list_source_files
    create_dir >> list_target_files
    [list_source_files, list_target_files] >> target_month
    target_month >> month_to_process
    month_to_process >> zip_path
    zip_path >> extract_and_upload_task
    extract_and_upload_task >> cleanup_task


etl_pipeline()
