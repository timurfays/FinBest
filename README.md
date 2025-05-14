
# FinBest: Платформа для анализа финансовых транзакций

## Описание
FinBest — это платформа для обработки и визуализации финансовых транзакций. Она использует **Apache Airflow** для оркестрации, **Apache Spark** для анализа графов, **DBT** для трансформации данных, **Superset** для создания дашбордов и **Jupyter** для интерактивной визуализации. Платформа обрабатывает данные транзакций, строит графы клиентов, выявляет подозрительные активности и генерирует отчёты.

## Структура репозитория
- **airflow/dags/**: DAG’и Airflow для ETL, маскировки PII и синхронизации с Superset.
  - `extract_load.py`, `mask_pii.py`, `transform.py`: Скрипты ETL.
  - `graph_analysis.py`, `superset_sync.py`: Анализ графов и синхронизация дашбордов.
- **data/**: Пример наборов данных (`bank_transactions.csv`, `clients.csv`).
- **dbt_project/**: Модели DBT для слоёв staging, intermediate и mart.
  - `models/staging/`: Исходные таблицы (`stg_clients.sql`, `stg_transactions.sql`).
  - `models/intermediate/`: Агрегации (`int_daily_transactions.sql`).
  - `models/mart/`: Финальные таблицы (`fact_client_balance.sql`).
- **init-scripts/postgres/**: SQL-скрипты для инициализации базы данных (`00_create_airflow_db.sql`, `01_schema.sql`).
- **notebooks/**: Jupyter-ноутбуки для визуализации графов (`graph_visualization.ipynb`).
- **spark/**: Скрипты Spark для построения графов и анализа.
  - `build_graph.py`: Строит граф транзакций.
  - `detect_suspicious.py`: Выявляет подозрительные активности.
- **superset/**: Конфигурация и настройка Superset.
  - `config/superset_config.py`: Файл конфигурации.
  - `setup_superset.py`: Скрипт инициализации.
- **Dockerfile**: Основной Dockerfile для Airflow и других сервисов.
- **docker-compose.yml**: Определяет сервисы (`airflow`, `spark`, `superset`, `jupyter`, `postgres`).
- **jupyter.Dockerfile**: Dockerfile для Jupyter.
- **spark.Dockerfile**: Dockerfile для Spark.
- **requirements.txt**, **requirements-base.txt**: Зависимости Python.
- **fix-permissions.sh**: Скрипт для исправления прав доступа (костыльный, см. раздел "Решение проблем").

## Требования
- **Docker** и **Docker Compose** (версия 2.0+).
- **Git**.
- Минимум 8 ГБ оперативной памяти для запуска всех сервисов.
- ОС: Linux (рекомендуется Ubuntu 20.04+), macOS или Windows с WSL2.

## Установка и запуск
### 1. Клонирование репозитория
```bash
git clone https://github.com/timurfays/FinBest.git
cd FinBest
```

### 2. Настройка окружения
- Убедитесь, что Docker и Docker Compose установлены:
  ```bash
  docker --version
  docker compose version
  ```

### 3. Сборка и запуск контейнеров
```bash
docker compose build
docker compose up -d
```
- Сборка занимает ~60 минут в зависимости от скорости интернета.
- После запуска сервисы будут доступны:
  - **Airflow**: `http://localhost:8080` (логин/пароль: `airflow`/`airflow`).
  - **Jupyter**: `http://localhost:8888` (токен - "finbest", смотрите в логах: `docker compose logs jupyter`).
  - **Superset**: `http://localhost:8088` (логин/пароль: `admin`/`admin`).
  - **Spark Master**: `http://localhost:8090`.
  - **Postgres**: `localhost:5432` (доступ через контейнеры).

### 4. Проверка статуса
```bash
docker compose ps
```
Все сервисы (`airflow-webserver`, `airflow-scheduler`, `jupyter`, `spark-master`, `spark-worker`, `superset`, `postgres`) должны быть в статусе `running` или `healthy`.

### 5. Запуск DAG’ов и визуализаций
- В Airflow активируйте DAG’и в веб-интерфейсе (`http://localhost:8080`).
- В Jupyter откройте `notebooks/graph_visualization.ipynb` для визуализации графов.
- В Superset настройте дашборды через `superset/setup_superset.py`.

## Решение проблем
### Ошибка `Permission denied` для логов Airflow
Airflow может упасть с ошибкой:
```
PermissionError: [Errno 13] Permission denied: '/opt/airflow/logs/scheduler'
```
**Причина**: Директория `./airflow/logs` отсутствует или недоступна для пользователя Airflow (UID 5000), который создаётся только после старта контейнеров.

**Решение**:
1. Запустите контейнеры, чтобы создался пользователь Airflow:
   ```bash
   docker compose up -d
   ```
2. Запустите скрипт `fix-permissions.sh` для предоставления полного доступа всем пользователям:
   ```bash
   sudo ./fix-permissions.sh
   ```
3. Если ошибка сохраняется, повторите запуск скрипта:
   ```bash
   sudo ./fix-permissions.sh
   ```
   **Внимание**: Скрипт `fix-permissions.sh` костыльный, так как пользователь Airflow (UID 5000) отсутствует до старта контейнеров. Повторный запуск скрипта после старта контейнеров обычно решает проблему.

4. Перезапустите Airflow:
   ```bash
   docker compose restart airflow-webserver airflow-scheduler
   ```

**Проверка**:
```bash
docker compose logs airflow-webserver
```
Убедитесь, что ошибка `Permission denied` исчезла.

### Лишние файлы в директории
Контейнеры могут создавать неотслеживаемые файлы (`airflow/logs`, `superset/superset.db`, `reports`), которые не нужны в репозитории.

**Решение**:
1. Убедитесь, что `.gitignore` включает:
   ```
   airflow/logs/
   superset/superset.db
   superset/data/
   reports/
   *.db
   __pycache__/
   *.pyc
   *.log
   ```
2. Удалите лишние файлы:
   ```bash
   rm -rf ./airflow/logs ./superset/superset.db ./superset/data ./reports
   ```

### Ошибки зависимостей в Jupyter
Если в `notebooks/graph_visualization.ipynb` возникают ошибки импорта (например, `NameError: name 'sns' is not defined` или `NameError: name 'rgb2hex' is not defined`):

**Решение**:
1. Убедитесь, что все зависимости установлены:
   ```bash
   docker compose exec jupyter pip list
   ```
   Проверьте наличие: `pandas`, `numpy==1.24.4`, `matplotlib`, `seaborn`, `plotly==5.18.0`, `pyvis==0.3.2`, `jupyter`, `networkx`, `python-louvain`.
2. Пересоберите Jupyter:
   ```bash
   docker compose build jupyter
   docker compose up -d jupyter
   ```
3. Проверьте импорты в ноутбуке:
   ```python
   import seaborn as sns
   from matplotlib.colors import to_hex
   ```

### Другие ошибки
- **Контейнеры не стартуют**: Проверьте логи:
  ```bash
  docker compose logs <service>
  ```
- **Конфликты портов**: Убедитесь, что порты 8080, 8088, 8888, 5432, 7077, 8090 свободны:
  ```bash
  sudo netstat -tuln | grep -E '8080|8088|8888|5432|7077|8090'
  ```
  Если заняты, остановите конфликтующие процессы или измените порты в `docker-compose.yml`.

## Откат к состоянию репозитория
Если в рабочей директории появились лишние файлы или изменения, которые не нужны:

1. **Остановите контейнеры**:
   ```bash
   docker compose down
   ```
2. **Удалите неотслеживаемые файлы**:
   ```bash
   git clean -fd
   ```
   **Внимание**: Это удалит все файлы и папки, не отслеживаемые Git (например, `airflow/logs`, `superset/superset.db`). Сохраните важные данные перед выполнением:
   ```bash
   mkdir /tmp/backup
   cp -r <важные_файлы> /tmp/backup
   ```
3. **Откатите изменения в отслеживаемых файлах**:
   ```bash
   git reset --hard
   ```
4. **Проверьте чистоту**:
   ```bash
   git status
   ```
   Должно быть:
   ```
   On branch main
   Your branch is up to date with 'origin/main'.
   nothing to commit, working tree clean
   ```
5. **Перезапустите контейнеры**:
   ```bash
   docker compose build
   docker compose up -d
   sudo ./fix-permissions.sh
   ```

## Дополнительные рекомендации
- **Логи**: Регулярно проверяйте логи сервисов:
  ```bash
  docker compose logs
  ```
- **Обновление зависимостей**: Если добавляете новые модули в `requirements-base.txt`, пересобирайте контейнеры:
  ```bash
  docker compose build
  ```
- **Резервные копии**: Перед внесением изменений сохраняйте копию ключевых файлов:
  ```bash
  cp notebooks/graph_visualization.ipynb notebooks/graph_visualization_backup.ipynb
  ```
- **Фиксация версий**: Для стабильности указывайте версии пакетов в `requirements-base.txt`, например:
  ```
  matplotlib==3.7.5
  seaborn==0.13.2
  ```

## Контакты
Если возникли проблемы, создайте issue в репозитории: [https://github.com/timurfays/FinBest](https://github.com/timurfays/FinBest).


