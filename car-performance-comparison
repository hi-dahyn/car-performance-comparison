# 이 프로젝트는 구글 코랩 환경에서 실행되었습니다

# PySpark 설치
!pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
from google.colab import files

# Spark 세션 생성
spark = SparkSession.builder.master("local[*]").getOrCreate()

# 파일 업로드
uploaded = files.upload()

# 업로드된 파일 이름 가져오기
file_name = list(uploaded.keys())[0]

# CSV 파일 읽기
df = spark.read.csv(file_name, header=True, inferSchema=True)

# 데이터 확인
df.show(5)
df.printSchema()

# 열 이름 확인
columns = df.columns
print("Columns: ", columns)

# 데이터 변환 (Parquet 형식으로 변환)
parquet_output_path = "/content/auto-data.parquet"
df.write.parquet(parquet_output_path)
print(f"Data saved in Parquet format at: {parquet_output_path}")

# Parquet 파일 읽기
df_parquet = spark.read.parquet(parquet_output_path)

# Transform 단계의 추가 작업들
# 열 이름이 'HP'라 가정하고 데이터를 변환합니다.
df_parquet = df_parquet.withColumn("HP", col("HP").cast("int"))

# 1. 특정 브랜드 데이터 필터링 (예: toyota)
df_toyota = df_parquet.filter(df_parquet["MAKE"].contains("toyota"))

# 2. 데이터 분리 및 분석
split_rdd = df_parquet.rdd.map(lambda row: row.asDict())

# 3. 중복 제거
distinct_df = df_parquet.distinct()

# 4. 집합 연산
# toyota 데이터프레임과 전체 데이터프레임의 합집합과 교집합을 계산
union_df = df_toyota.union(distinct_df).distinct()
intersection_df = df_toyota.intersect(distinct_df)

# 5. 사용자 정의 함수 사용
# 특정 필드를 대문자로 변환
transformed_df = df_parquet.withColumn("MAKE", upper(df_parquet["MAKE"]))

# Load (데이터 적재)
# 변환된 데이터를 Parquet 형식으로 다시 저장
transformed_output_path = "/content/transformed-auto-data.parquet"
transformed_df.write.parquet(transformed_output_path)
print(f"Transformed data saved in Parquet format at: {transformed_output_path}")

# Parquet 파일 읽기
transformed_df_parquet = spark.read.parquet(transformed_output_path)

# 집계 및 분석
# 1. Reduce 연산을 통한 집계
# 예시로 레코드의 총합 계산 (여기서는 문자열 길이의 총합으로 예시)
total_length = transformed_df_parquet.rdd.map(lambda row: len(str(row))).reduce(lambda a, b: a + b)
print(f"Total length of records: {total_length}")

# 2. 키-값 쌍 생성 및 분석
# 각 브랜드와 마력을 키-값 쌍으로 변환하고, 브랜드별로 마력의 총합과 평균을 계산
def parse_row(row):
    fields = row.asDict()
    brand = fields["MAKE"]
    horsepower = fields["HP"] if fields["HP"] is not None else 0
    return (brand, horsepower)

brand_horsepower_rdd = transformed_df_parquet.rdd.map(parse_row)
brand_horsepower_sum = brand_horsepower_rdd.reduceByKey(lambda a, b: a + b)
brand_horsepower_avg = brand_horsepower_rdd.mapValues(lambda v: (v, 1)).reduceByKey(lambda a, b: (a[0] + b[0], a[1])).mapValues(lambda v: v[0] / v[1])

print("Brand horsepower sum:")
print(brand_horsepower_sum.collect())

print("Brand horsepower average:")
print(brand_horsepower_avg.collect())
