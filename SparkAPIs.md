### Đọc & ghi dữ liệu (I/O)
| API                          | Mô tả                                |
| ---------------------------- | ------------------------------------ |
| `spark.read.csv()`           | Đọc file CSV                         |
| `spark.read.json()`          | Đọc file JSON                        |
| `spark.read.parquet()`       | Đọc file Parquet                     |
| `df.write.csv()`             | Ghi DataFrame thành CSV              |
| `df.write.parquet()`         | Ghi DataFrame thành Parquet          |
| `df.write.format("mongodb")` | Ghi vào MongoDB (khi dùng connector) |
```bash
df_csv = spark.read.option("header", True).csv("retail.csv")
df_csv.show(5)

df_json = spark.read.json("sample_data.json")
df_json.show(5)

df_parquet = spark.read.parquet("data/output.parquet")
df_parquet.show(5)

df_csv.write.option("header", True).mode("overwrite").csv("output_csv/")

df_csv.write.mode("overwrite").parquet("output_parquet/")
```

### Khởi tạo & xem dữ liệu
| API                | Mô tả                 |
| ------------------ | --------------------- |
| `df.show()`        | Hiển thị vài dòng đầu |
| `df.printSchema()` | In sơ đồ dữ liệu      |
| `df.describe()`    | Thống kê cơ bản       |
| `df.columns`       | Danh sách tên cột     |
| `df.schema`        | Kiểu dữ liệu các cột  |
```bash
linesDF = spark.read.text("CSC14118/ppap.txt")
linesDF.show(linesDF.count(),truncate = False)
# linesDF.count(): đếm tổng số dòng trong file.
# show(n): hiển thị n dòng đầu tiên trong DataFrame.
# truncate=False: hiển thị đầy đủ nội dung từng dòng, không bị cắt ngắn (mặc định là truncate=True thì các dòng dài sẽ bị cắt).
```
### Thư viện functions
| API                            | Mô tả                        |
| ------------------------------ | ---------------------------- |
| `length("col")`                | Lấy số kí tự trong chuỗi     |
| `size("col")`                  | Lấy kích thước của list      |
| `F.array_contains("col", value)`| Kiểm tra xem value có trong col không      |
| `col("name")`                  | Đại diện cho một cột         |
| `lit(value)`                   | Tạo cột giá trị cố định      |
| `when(cond, val)`              | Tương đương `if` – điều kiện |
| `sum("col")`                   | Tổng giá trị                 |
| `avg("col")`                   | Trung bình                   |
| `count("col")`                 | Đếm                          |
| `lower("col")`, `upper("col")` | Đổi chữ hoa/thường           |
| `substring("col", start, len)` | Cắt chuỗi                    |
| `explode()`                    | Tách mảng thành từng dòng    |
| `split()`                      | Tách chuỗi thành mảng        |
```bash
from pyspark.sql import functions as F

df.select(F.col("name")).show()
# Lấy duy nhất cột name.

df.withColumn("constant", F.lit(1)).show()
# Tạo cột mới "constant" có giá trị cố định 1

df.withColumn(
    "grade",
    F.when(F.col("score") >= 300, "High")
     .when(F.col("score") >= 200, "Medium")
     .otherwise("Low")
).show()
# Phân loại điểm số theo điều kiện

df.agg(F.sum("score").alias("TotalScore")).show()
# Tổng toàn bộ cột score

df.agg(F.count("name").alias("TotalRows")).show()
# Đếm số dòng (không null) trong cột name.

df.select(F.upper(F.col("name")).alias("Name_Upper")).show()
df.select(F.lower(F.col("name")).alias("Name_Lower")).show()
# Đổi toàn bộ tên thành chữ hoa/thường.

df.select(F.substring("name", 1, 3).alias("ShortName")).show()
# Lấy 3 ký tự đầu tiên của name.

df_split = df.withColumn("tag_array", F.split("tags", ","))
df_split.select("name", F.explode("tag_array").alias("tag")).show()
# Tách chuỗi tags thành mảng rồi explode thành từng dòng riêng.

df.withColumn("tag_array", F.split("tags", ",")).show()
# Tách chuỗi "A,B,C" thành ["A", "B", "C"].
```
### Biến đổi dữ liệu (Transformations)
| API                                      | Mô tả                 |
| ---------------------------------------- | --------------        |
| `df.first()["col"]                       | list val of fist col  |
| `df.toPandas().["col"].tolist()          | list val of col       |
| `df.select("col")`                       | Chọn cột              |
| `df.withColumn("new_col", expr)`         | Thêm/sửa cột          |
| `df.drop("col")`                         | Xoá cột               |
| `df.filter(condition)` hoặc `df.where()` | Lọc dòng              |
| `df.distinct()`                          | Lọc dòng trùng        |
| `df.dropDuplicates()`                    | Xoá dòng trùng        |
| `df.orderBy("col")`                      | Sắp xếp               |       
| `df.limit(n)`                            | Giới hạn dòng         |
| `df.sort("col", ascending=True/False)    | Sắp xếp               |
```bash
df.select("CustomerID", "Quantity").show(5)

# df.withColumn()
from pyspark.sql.functions import col
df = df.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

df = df.drop("Description")

df.filter(col("Country") == "United Kingdom").show(5)
# hoặc
df.where(col("Quantity") > 10).show(5)

# Trả về danh sách quốc gia không trùng lặp
df.select("Country").distinct().show()

# Chỉ giữ lại dòng đầu tiên trong các dòng có cùng InvoiceNo và StockCode
df.dropDuplicates(["InvoiceNo", "StockCode"]).show()

df.orderBy("Quantity").show(5)
df.orderBy(col("UnitPrice").desc()).show(5)

df.limit(10).show()

df.sort("Quantity", ascending=False)
```
### Nhóm & tổng hợp (Aggregation)
| API                                                 | Mô tả              |
| --------------------------------------------------- | ------------------ |
| `df.groupBy("col").agg(...)`                        | Nhóm và tổng hợp   |
| `agg({"col": "sum"})`                               | Tổng hợp từng cột  |
| `df.groupBy("col").count()`                         | Đếm dòng theo nhóm |
| `from pyspark.sql.functions import avg, sum, count` | Các hàm tổng hợp   |
```bash
# Ví dụ: Tổng tiền mua theo từng khách hàng
df.groupBy("CustomerID").agg({"TotalPrice": "sum"}).show(5)

# Tính tổng Quantity và tổng UnitPrice
df.agg({"Quantity": "sum", "UnitPrice": "sum"}).show()

# Đếm số lượng đơn hàng theo từng quốc gia
df.groupBy("Country").count().show(5)

from pyspark.sql.functions import avg, sum, count
# Tính trung bình và tổng tiền mua theo từng khách hàng
df.groupBy("CustomerID").agg(
    sum("TotalPrice").alias("TotalSpent"),
    avg("TotalPrice").alias("AvgSpent"),
    count("*").alias("NumOrders")
).show(5)
```
### Join dữ liệu
| API                                    | Mô tả                           |
| -------------------------------------- | ------------------------------- |
| `df1.join(df2, on="col")`              | Join mặc định (inner)           |
| `df1.join(df2, on="col", how="left")`  | Left join                       |
| `df1.join(df2, on="col", how="right")` | Right join                      |
| `df1.join(df2, on="col", how="outer")` | Full outer join                 |
| `df1.join(df2, on="col", how="anti")`  | Anti join (các phần không khớp) |
### Window functions (Hàm cửa sổ)
| API                             | Mô tả                |
| ------------------------------- | -------------------- |
| `Window.partitionBy("col")`     | Chia nhóm cho cửa sổ |
| `Window.orderBy("col")`         | Sắp xếp trong cửa sổ |
| `row_number().over(windowSpec)` | Số thứ tự từng dòng  |
| `rank().over(windowSpec)`       | Hạng có trùng        |
| `dense_rank().over(windowSpec)` | Hạng không trùng     |
```bash
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank

spark = SparkSession.builder.appName("WindowFunctionExample").getOrCreate()

# Tạo DataFrame mô phỏng điểm thi của học sinh theo lớp
data = [
    Row(Class="A", Student="Anna", Score=95),
    Row(Class="A", Student="Ben", Score=87),
    Row(Class="A", Student="Cindy", Score=95),
    Row(Class="B", Student="David", Score=91),
    Row(Class="B", Student="Eva", Score=91),
    Row(Class="B", Student="Frank", Score=85),
]
df = spark.createDataFrame(data)
df.show()
```
```bash
from pyspark.sql.window import Window

# Tạo cửa sổ chia theo lớp học
windowSpec = Window.partitionBy("Class").orderBy(df["Score"].desc())

# Mỗi "Class" là một nhóm riêng, sau đó sắp xếp học sinh theo điểm số giảm dần.
```
```bash
df.withColumn("row_number", row_number().over(windowSpec)).show()

# Mỗi học sinh được đánh số thứ tự trong lớp mình, dù điểm có trùng cũng vẫn tăng số thứ tự (1, 2, 3,...).
```
```bash
df.withColumn("rank", rank().over(windowSpec)).show()

# Trùng điểm → trùng hạng
# Dòng sau nhảy hạng (ví dụ: hạng 1, 1, 3)
```
```bash
df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()

# Trùng điểm → trùng hạng
# Nhưng không bỏ qua hạng (ví dụ: hạng 1, 1, 2)
```








