# Spark MLlib FPGrowth
Spark MLlib FPGrowth là một thuật toán mạnh mẽ để khai phá tập mục thường xuyên và luật kết hợp (association rules), thường dùng trong các bài toán như:
- Gợi ý sản phẩm (như Amazon, Shopee)
- Phân tích giỏ hàng (Market Basket Analysis)
- Khai phá quy luật mua sắm

# 📚 Giới thiệu ngắn về FPGrowth trong Spark
- Tên đầy đủ: Frequent Pattern Growth
- Mục tiêu: Tìm ra các tập item phổ biến xuất hiện cùng nhau trong các giao dịch
- Là một phần của thư viện pyspark.ml.fpm.FPGrowth (thuộc spark.ml, không phải spark.mllib cũ)

# Cài đặt và chuẩn bị môi trường
```bash
!apt-get install openjdk-11-jdk -y
!wget -q https://downloads.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
!tar xf spark-3.4.1-bin-hadoop3.tgz
!pip install -q findspark

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.4.1-bin-hadoop3"

import findspark
findspark.init()
```
# Ví dụ đơn giản với FPGrowth
```bash
from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth

spark = SparkSession.builder.appName("FPGrowth Example").getOrCreate()

# Dữ liệu giao dịch
data = [
    (0, ["milk", "bread", "butter"]),
    (1, ["bread", "butter"]),
    (2, ["milk", "bread"]),
    (3, ["milk", "bread", "butter", "apple"]),
    (4, ["bread", "butter"])
]

# Tạo DataFrame
df = spark.createDataFrame(data, ["id", "items"])

# Huấn luyện mô hình FPGrowth
fpGrowth = FPGrowth(itemsCol="items", minSupport=0.5, minConfidence=0.6)
model = fpGrowth.fit(df)

# In các itemsets thường xuyên
model.freqItemsets.show()

# In luật kết hợp
model.associationRules.show()

# Dự đoán các item có thể mua thêm cho user mới
model.transform(df).show()
```
# Định dạng dữ liệu yêu cầu bởi Spark MLlib FPGrowth trong thư viện pyspark.ml.fpm.FPGrowth:
Là một DataFrame có 2 cột:
- ID (tuỳ chọn): IntegerType hoặc StringType – có thể là transaction_id hoặc user_id.
- Items (bắt buộc): một cột kiểu array<string>, trong đó mỗi dòng là một danh sách các item của một giao dịch.

Giả sử có dataframe như sau:

|Acetominifen|Anchovies|Aspirin|Auto Magazines|Bagels|
|------------|---------|-------|--------------|------|
|           1|        0|      0|             0|     0|
|           1|        0|      0|             0|     0|
|           0|        0|      0|             0|     0|
|           0|        0|      0|             0|     0|

Ta chuyển về dạng chuẩn bằng code
```bash
def get_items(row):
    return [item for item in item_columns if row[item] == 1]

# Tạo cột 'items' từ các cột one-hot
df_items = df.rdd.zipWithIndex().map(lambda x: (x[1], get_items(x[0]))).toDF(["id", "items"])
df_items.show(truncate=False)
```
- df.rdd: chuyển DataFrame sang RDD<Row>
- .zipWithIndex(): gán số thứ tự dòng (index) cho mỗi row → Trả về RDD gồm các phần tử dạng (row, index)
- .map(lambda x: (x[1], get_items(x[0]))):
  - x[0]: là row
  - x[1]: là index (id)
- Áp dụng get_items(x[0]) để lấy danh sách sản phẩm có trong giao dịch → Tạo tuple (id, [items])
- .toDF(["id", "items"]): chuyển RDD kết quả thành lại DataFrame có 2 cột:
  - id: số thứ tự giao dịch
  - items: danh sách sản phẩm
# Apply Spark MLlib FPGrowth to the formatted data. Mine the set of frequent patterns with the minimum support of 0.1. Mine the set of association rules with the minimum confidence of 0.9.
```bash
fpGrowth = FPGrowth(
    itemsCol="items",
    minSupport=0.1,
    minConfidence=0.9
)

model = fpGrowth.fit(df_items)

# Các tập item phổ biến (frequent itemsets)
model.freqItemsets.show(truncate=False)

# Luật kết hợp (association rules)
model.associationRules.show(truncate=False)

# Dự đoán các item có thể gợi ý thêm cho người dùng
model.transform(df_items).show(truncate=False)
```
# Các functions thường gặp của pyspark.ml
1. Transformer

| Tên                | Mô tả                                                   | Ví dụ                                      |
| ------------------ | ------------------------------------------------------- | ------------------------------------------ |
| `Tokenizer`        | Tách văn bản thành danh sách từ                         | Từ `"hello world"` → `["hello", "world"]`  |
| `RegexTokenizer`   | Tách từ dựa trên regex                                  | Xử lý tốt hơn `Tokenizer`                  |
| `StopWordsRemover` | Loại bỏ các từ dừng (a, the, of, ...)                   | Dùng sau Tokenizer                         |
| `HashingTF`        | Chuyển danh sách từ thành vector đặc trưng bằng hashing | Vector hóa dữ liệu văn bản                 |
| `CountVectorizer`  | Vector hóa từ theo tần suất (bag of words)              | Có thể học từ dữ liệu                      |
| `IDF`              | Tính trọng số TF-IDF từ đầu ra của CountVectorizer      | Loại bỏ từ phổ biến                        |
| `StringIndexer`    | Biến chuỗi thành số (label encoding)                    | `"Male"` → `0`, `"Female"` → `1`           |
| `OneHotEncoder`    | Biến số thứ tự thành vector nhị phân                    | `1` → `[0, 1, 0]`                          |
| `VectorAssembler`  | Gộp nhiều cột đặc trưng thành 1 vector                  | Đầu vào cho mô hình                        |
| `StandardScaler`   | Chuẩn hóa dữ liệu (mean=0, std=1)                       | Đặc biệt quan trọng với mô hình tuyến tính |
| `MinMaxScaler`     | Đưa dữ liệu về \[0, 1]                                  |                                            |
| `PCA`              | Giảm chiều dữ liệu                                      |                                            |
| `ChiSqSelector`    | Chọn đặc trưng bằng thống kê Chi-Square                 | Dùng cho bài toán phân loại                |
```bash
from pyspark.ml.feature import Tokenizer
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TokenizerExample").getOrCreate()
df = spark.createDataFrame([(0, "hello world"), (1, "machine learning is fun")], ["id", "text"])
tokenizer = Tokenizer(inputCol="text", outputCol="words")
tokenized = tokenizer.transform(df)
tokenized.show(truncate=False)
```
```bash
from pyspark.ml.feature import RegexTokenizer

regex_tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")
regex_tokenized = regex_tokenizer.transform(df)
regex_tokenized.show(truncate=False)
```
```bash
from pyspark.ml.feature import StopWordsRemover

remover = StopWordsRemover(inputCol="words", outputCol="filtered")
filtered_df = remover.transform(tokenized)
filtered_df.show(truncate=False)
```
```bash
from pyspark.ml.feature import HashingTF

hashingTF = HashingTF(inputCol="filtered", outputCol="features", numFeatures=20)
featurized = hashingTF.transform(filtered_df)
featurized.show(truncate=False)
```
```bash
from pyspark.ml.feature import CountVectorizer, IDF

cv = CountVectorizer(inputCol="filtered", outputCol="rawFeatures")
cv_model = cv.fit(filtered_df)
cv_featurized = cv_model.transform(filtered_df)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idf_model = idf.fit(cv_featurized)
rescaled = idf_model.transform(cv_featurized)
rescaled.select("filtered", "features").show(truncate=False)
```
```bash
from pyspark.ml.feature import StringIndexer

df_gender = spark.createDataFrame([("Male",), ("Female",), ("Female",)], ["gender"])
indexer = StringIndexer(inputCol="gender", outputCol="genderIndex")
df_indexed = indexer.fit(df_gender).transform(df_gender)
df_indexed.show()
```
```bash
from pyspark.ml.feature import OneHotEncoder

encoder = OneHotEncoder(inputCols=["genderIndex"], outputCols=["genderVec"])
encoded = encoder.fit(df_indexed).transform(df_indexed)
encoded.show()
```
```bash
from pyspark.ml.feature import VectorAssembler

df_features = spark.createDataFrame([(1.0, 2.0, 3.0)], ["f1", "f2", "f3"])
assembler = VectorAssembler(inputCols=["f1", "f2", "f3"], outputCol="features")
assembled = assembler.transform(df_features)
assembled.show()
```
```bash
from pyspark.ml.feature import StandardScaler

scaler = StandardScaler(inputCol="features", outputCol="scaled", withMean=True, withStd=True)
scaler_model = scaler.fit(assembled)
scaled_data = scaler_model.transform(assembled)
scaled_data.show()
```
```bash
from pyspark.ml.feature import MinMaxScaler

scaler = MinMaxScaler(inputCol="features", outputCol="scaled")
model = scaler.fit(assembled)
scaled = model.transform(assembled)
scaled.show()
```
```bash
from pyspark.ml.feature import PCA

pca = PCA(k=2, inputCol="features", outputCol="pcaFeatures")
pca_model = pca.fit(assembled)
pca_result = pca_model.transform(assembled)
pca_result.select("pcaFeatures").show(truncate=False)
```
```bash
from pyspark.ml.feature import ChiSqSelector
from pyspark.ml.linalg import Vectors

df = spark.createDataFrame([
    (1, Vectors.dense([0.0, 0.0, 18.0]), 1.0),
    (2, Vectors.dense([1.0, 1.0, 5.0]), 0.0),
    (3, Vectors.dense([2.0, 1.0, 8.0]), 1.0)
], ["id", "features", "label"])

selector = ChiSqSelector(numTopFeatures=2, featuresCol="features", outputCol="selectedFeatures", labelCol="label")
selected = selector.fit(df).transform(df)
selected.select("features", "selectedFeatures").show()
```
2. Estimators (mô hình huấn luyện)

| Mô hình                  | Mô tả                                 |
| ------------------------ | ------------------------------------- |
| `LogisticRegression`     | Phân loại nhị phân                    |
| `LinearRegression`       | Hồi quy tuyến tính                    |
| `DecisionTreeClassifier` | Cây quyết định phân loại              |
| `RandomForestClassifier` | Rừng ngẫu nhiên                       |
| `GBTClassifier`          | Gradient Boosted Tree                 |
| `NaiveBayes`             | Phân loại Naive Bayes                 |
| `KMeans`                 | Phân cụm K-Means                      |
| `FPGrowth`               | Khai phá luật kết hợp                 |
| `ALS`                    | Collaborative filtering (recommender) |
```bash
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.sql import Row

spark = SparkSession.builder.appName("LogisticRegression").getOrCreate()

data = [Row(label=0.0, features=Vectors.dense(1.0, 0.1)),
        Row(label=1.0, features=Vectors.dense(2.0, 1.1)),
        Row(label=0.0, features=Vectors.dense(1.3, 0.2))]

df = spark.createDataFrame(data)
lr = LogisticRegression(featuresCol="features", labelCol="label")
model = lr.fit(df)
model.transform(df).show()
```
```bash
from pyspark.ml.regression import LinearRegression

data = [(1.0, Vectors.dense(1.0)), (2.0, Vectors.dense(2.0)), (3.0, Vectors.dense(3.0))]
df = spark.createDataFrame(data, ["label", "features"])
lr = LinearRegression()
model = lr.fit(df)
model.transform(df).show()
```
```bash
from pyspark.ml.classification import DecisionTreeClassifier

data = [(0.0, Vectors.dense(0.0, 1.0)),
        (1.0, Vectors.dense(1.0, 0.0)),
        (0.0, Vectors.dense(0.0, 0.0))]
df = spark.createDataFrame(data, ["label", "features"])
dt = DecisionTreeClassifier()
model = dt.fit(df)
model.transform(df).show()
```
```bash
from pyspark.ml.classification import RandomForestClassifier

rf = RandomForestClassifier(numTrees=10)
model = rf.fit(df)
model.transform(df).show()
```
```bash
from pyspark.ml.classification import GBTClassifier

gbt = GBTClassifier()
model = gbt.fit(df)
model.transform(df).show()
```
```bash
from pyspark.ml.classification import NaiveBayes

df_nb = spark.createDataFrame([
    (0.0, Vectors.dense([1.0, 0.0])),
    (1.0, Vectors.dense([0.0, 1.0])),
    (0.0, Vectors.dense([1.0, 0.2]))
], ["label", "features"])

nb = NaiveBayes()
model = nb.fit(df_nb)
model.transform(df_nb).show()
```
```bash
from pyspark.ml.clustering import KMeans

df_kmeans = spark.createDataFrame([
    (Vectors.dense([1.0, 1.0]),),
    (Vectors.dense([9.0, 8.0]),),
    (Vectors.dense([1.2, 1.1]),),
], ["features"])

kmeans = KMeans(k=2)
model = kmeans.fit(df_kmeans)
model.transform(df_kmeans).show()
```
```bash
from pyspark.ml.fpm import FPGrowth

df_fpg = spark.createDataFrame([
    (0, ["milk", "bread"]),
    (1, ["milk", "diaper", "beer", "bread"]),
    (2, ["milk", "diaper", "bread", "cola"]),
], ["id", "items"])

fp = FPGrowth(itemsCol="items", minSupport=0.5, minConfidence=0.6)
model = fp.fit(df_fpg)
model.freqItemsets.show()
model.associationRules.show()
```
```bash
from pyspark.ml.recommendation import ALS

ratings = spark.createDataFrame([
    (0, 0, 4.0),
    (0, 1, 2.0),
    (1, 1, 3.0),
    (1, 2, 4.0),
], ["user", "item", "rating"])

als = ALS(userCol="user", itemCol="item", ratingCol="rating", coldStartStrategy="drop")
model = als.fit(ratings)
model.recommendForAllUsers(2).show(truncate=False)
```
3. Pipeline & Evaluation
   
| Thành phần                               | Mô tả                                                |
| ---------------------------------------- | ---------------------------------------------------- |
| `Pipeline`                               | Chuỗi các bước xử lý từ Transformer đến Estimator    |
| `PipelineModel`                          | Mô hình pipeline sau khi fit                         |
| `TrainValidationSplit`, `CrossValidator` | Tách tập huấn luyện/kiểm tra để tìm tham số tốt nhất |
| `BinaryClassificationEvaluator`          | Đánh giá mô hình nhị phân                            |
| `MulticlassClassificationEvaluator`      | Đánh giá đa lớp                                      |
| `RegressionEvaluator`                    | Đánh giá hồi quy                                     |

**✅ Bài toán: Phân loại nhị phân văn bản (ham vs spam) với LogisticRegression**
```bash
# Import các thư viện cần thiết
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
```
```bash
# Tạo SparkSession và dữ liệu mẫu
spark = SparkSession.builder.appName("TextClassificationPipeline").getOrCreate()

data = spark.createDataFrame([
    (0, "free money now", 1),
    (1, "hi how are you", 0),
    (2, "win big prize", 1),
    (3, "hello let's meet", 0),
], ["id", "text", "label"])
```
```bash
# Xây dựng Pipeline
tokenizer = Tokenizer(inputCol="text", outputCol="words")
stopwords = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=20)
idf = IDF(inputCol="rawFeatures", outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="label")

pipeline = Pipeline(stages=[tokenizer, stopwords, hashingTF, idf, lr])
```
```bash
# TrainValidationSplit để tìm tham số tốt nhất
paramGrid = ParamGridBuilder() \
    .addGrid(hashingTF.numFeatures, [10, 20, 30]) \
    .addGrid(lr.regParam, [0.1, 0.01]) \
    .build()

evaluator = BinaryClassificationEvaluator(labelCol="label")

tvs = TrainValidationSplit(estimator=pipeline,
                           estimatorParamMaps=paramGrid,
                           evaluator=evaluator,
                           trainRatio=0.8)
```
```bash
# Huấn luyện mô hình & đánh giá
model = tvs.fit(data)  # Đây là PipelineModel

result = model.transform(data)
result.select("id", "text", "label", "probability", "prediction").show()

auc = evaluator.evaluate(result)
print("AUC =", auc)
```
🔁 Nếu là phân loại đa lớp: dùng MulticlassClassificationEvaluator  
🔁 Nếu là hồi quy: dùng RegressionEvaluator
4. Hàm tiện ích / kiểm tra

| Hàm            | Mô tả                                 |
| -------------- | ------------------------------------- |
| `.fit()`       | Huấn luyện mô hình                    |
| `.transform()` | Áp dụng mô hình hoặc transformer      |
| `.evaluate()`  | Đánh giá hiệu suất                    |
| `.getStages()` | Lấy danh sách các bước trong pipeline |
| `.randomSplit()| Chia dữ liệu thành train/test         |
```bash
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
```
