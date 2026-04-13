# BigDataSpark - Лабораторная работа №2

ETL пайплайн: CSV -> PostgreSQL (Star Schema) -> ClickHouse (Reports)

## Подгот

**1. Запуск:**
```
docker-compose up -d
```

**2. ETL в модель Звезда:**
```
docker exec bigdata_spark_master spark-submit /opt/spark-jobs/etl_to_star_schema.py
```

**3. Создание отчетов:**
```
docker exec bigdata_spark_master spark-submit /opt/spark-jobs/etl_to_clickhouse.py
```

## Отчеты

1. report_products - продажи по продуктам
2. report_customers - покупательское поведение
3. report_time - временные тренды
4. report_stores - эффективность магазинов
5. report_suppliers - эффективность поставщиков
6. report_quality - качество продукции

## Остановка

```
docker-compose down
```

Выполнил: **Степанов Никита Евгеньевич**, группа **М8О-308Б-23**
