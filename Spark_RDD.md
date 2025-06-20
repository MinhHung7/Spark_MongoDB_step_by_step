### Tạo RDD
| API                                                     | Mô tả                                |
| ----------------------------                            | ------------------------------------ |
| `spark.sparkContext.textFile()`                         | Đọc file txt                         |
| `spark.sparkContext.parallelize(data, numSlices=None)`  |	Tạo RDD từ danh sách Python          |
```bash
linesRdd = spark.sparkContext.textFile("CSC14118/ppap.txt")
linesRdd.collect()
# ['ppap',
# 'i have a pen',
# 'i have an apple',
# 'ah apple pen',
# 'i have a pen',
# 'i have a pineapple',
# 'ah pineapple pen',
# 'ppap pen pineapple apple pen']


rdd = sc.parallelize(data, numSlices=None)
# data: là danh sách (list), tuple, range, v.v.
# numSlices: số partition bạn muốn chia dữ liệu (tùy chọn). Nếu không truyền, Spark sẽ tự quyết định.
```
# Transformation
| Hàm                  | Mô tả                        | Ví dụ                                 |
| -------------------- | ---------------------------- | ------------------------------------- |
| `map(func)`          | Áp dụng hàm lên từng phần tử | `rdd.map(lambda x: x + 1)`            |
| `flatMap(func)`      | Tách phần tử → nhiều phần tử | `rdd.flatMap(lambda x: x.split(" "))` |
| `filter(func)`       | Lọc phần tử theo điều kiện   | `rdd.filter(lambda x: x > 5)`         |
| `distinct()`         | Lọc trùng                    | `rdd.distinct()`                      |
| `sample()`           | Lấy mẫu ngẫu nhiên           | `rdd.sample(False, 0.5)`              |
| `union(rdd2)`        | Gộp 2 RDD lại                | `rdd1.union(rdd2)`                    |
| `zip(rdd2)`          | Bắt cặp từng phần tử lại     | `rdd1.zip(rdd2)`                      |
| `intersection(rdd2)` | Giao 2 RDD                   | `rdd1.intersection(rdd2)`             |
| `cartesian(rdd2)`    | Tích Descartes               | `rdd1.cartesian(rdd2)`                |
| `isEmpty()`          | Kiểm tra có rỗng không       | `rdd.isEmpty()`                       |
```bash
rdd = sc.parallelize([1, 2, 3, 4])
rdd.map(lambda x: x * 2).collect()  # → [2, 4, 6, 8]
# Áp dụng hàm lên từng phần tử

lines = spark.sparkContext.parallelize(["hello world", "spark rdd", "spark"])
lines.flatMap(lambda line: line.split(" ")).collect()
# → ['hello', 'world', 'spark', 'rdd', 'spark']
# Tách phần tử thành nhiều phần tử

rdd = sc.parallelize([1, 2, 3, 4, 5])
rdd.filter(lambda x: x % 2 == 0).collect()  # → [2, 4]
# Giữ lại phần tử thỏa điều kiện

rdd = sc.parallelize([1, 2, 2, 3, 3, 3])
rdd.distinct().collect()  # → [1, 2, 3]
# Lọc trùng

rdd = sc.parallelize([1, 2, 3, 4, 5, 6])
rdd.sample(False, 0.5).collect()  # → ví dụ: [2, 5, 6]
# Lấy mẫu ngẫu nhiên
# withReplacement=False: không chọn trùng
# fraction=0.5: khoảng 50% dữ liệu được chọn

rdd1 = sc.parallelize([1, 2, 3])
rdd2 = sc.parallelize([3, 4, 5])
rdd1.union(rdd2).collect()  # → [1, 2, 3, 3, 4, 5]
# Gộp 2 RDD

rdd1 = sc.parallelize([1, 2, 3])
rdd2 = sc.parallelize([2, 3, 4])
rdd1.intersection(rdd2).collect()  # → [2, 3]
# Giao 2 RDD

rdd1 = sc.parallelize([1, 2])
rdd2 = sc.parallelize(['a', 'b'])
rdd1.cartesian(rdd2).collect()
# → [(1, 'a'), (1, 'b'), (2, 'a'), (2, 'b')]
# Tích Descartes
```
# Transformation trên key-value (Pair RDD)
| Hàm                   | Mô tả                       | Ví dụ                                       |
| --------------------- | --------------------------- | ------------------------------------------- |
| `mapValues(func)`     | Áp dụng hàm lên value       | `rdd.mapValues(lambda v: v+1)`              |
| `reduceByKey(func)`   | Gộp value theo key          | `rdd.reduceByKey(lambda a, b: a + b)`       |
| `groupByKey()`        | Gom nhóm value theo key     | `rdd.groupByKey()`                          |
| `sortByKey()`         | Sắp xếp theo key            | `rdd.sortByKey()`                           |
| `sortBy()`            | Sắp xếp tự do               | `rdd.sortBy()`                              |
| `flatMapValues(func)` | Tách value thành nhiều dòng | `rdd.flatMapValues(lambda v: v.split(","))` |
| `join(rdd2)`          | Join theo key               | `rdd1.join(rdd2)`                           |
| `leftOuterJoin(rdd2)` | Left join                   | `rdd1.leftOuterJoin(rdd2)`                  |
```bash
rdd = sc.parallelize([("a", 1), ("b", 2)])
rdd.mapValues(lambda v: v + 10).collect()
# → [('a', 11), ('b', 12)]

rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 3)])
rdd.reduceByKey(lambda a, b: a + b).collect()
# → [('a', 4), ('b', 2)]
# Gộp các value theo key (nhóm + cộng dồn)
# Efficient hơn groupByKey() vì gộp sớm, ít tốn bộ nhớ.

rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 3)])
grouped = rdd.groupByKey().mapValues(list)
grouped.collect()
# → [('a', [1, 3]), ('b', [2])]
# Gom tất cả value theo key thành iterable
# Cẩn thận: dùng với tập dữ liệu lớn có thể gây out-of-memory.

rdd = sc.parallelize([("b", 2), ("a", 1), ("c", 3)])
rdd.sortByKey().collect()
# → [('a', 1), ('b', 2), ('c', 3)]
# Sắp xếp theo key, mặc định tăng dần

rdd = sc.parallelize([("b", 2), ("a", 1), ("c", 3)])
rdd.sortBy(lambda pair: -1 * pair[1]).collect()
# → [('c', 3), ('b', 2), ('a', 1)]
# Sắp xếp theo value, giảm dần

rdd = sc.parallelize([("fruit", "apple,banana"), ("veg", "carrot")])
rdd.flatMapValues(lambda v: v.split(",")).collect()
# → [('fruit', 'apple'), ('fruit', 'banana'), ('veg', 'carrot')]
# Tách value thành nhiều dòng, giữ key gốc

rdd1 = sc.parallelize([("a", 1), ("b", 2)])
rdd2 = sc.parallelize([("a", "A"), ("b", "B")])
rdd1.join(rdd2).collect()
# → [('a', (1, 'A')), ('b', (2, 'B'))]
# Inner join theo key

rdd1 = sc.parallelize([("a", 1), ("b", 2)])
rdd2 = sc.parallelize([("a", "A")])
rdd1.leftOuterJoin(rdd2).collect()
# → [('a', (1, 'A')), ('b', (2, None))]
# Giữ key từ rdd1, join nếu có ở rdd2
```







