# 🚀 Tổng quan về GraphFrames
GraphFrames là một thư viện cho Spark hỗ trợ xử lý đồ thị, giống như NetworkX nhưng phân tán (có thể xử lý dữ liệu cực lớn). Nó hoạt động với DataFrame để đại diện cho đỉnh (vertices) và cạnh (edges).
- Mỗi đỉnh là một dòng trong vertices DataFrame (id, name, v.v.).
- Mỗi cạnh là một dòng trong edges DataFrame (src, dst, relationship, v.v.).

# 🧰 Các hàm phổ biến của GraphFrame (và ví dụ)
| Function                          | Mô tả ngắn                                            | Ví dụ                                          |
| --------------------------------- | ----------------------------------------------------- | ---------------------------------------------- |
| `g.vertices`                      | Truy cập danh sách đỉnh                               | `g.vertices.show()`                            |
| `g.edges`                         | Truy cập danh sách cạnh                               | `g.edges.show()`                               |
| `g.inDegrees`                     | Đếm số cạnh đến (in-degree) cho mỗi đỉnh              | `g.inDegrees.show()`                           |
| `g.outDegrees`                    | Đếm số cạnh đi (out-degree) cho mỗi đỉnh              | `g.outDegrees.show()`                          |
| `g.degrees`                       | Tổng inDegree + outDegree                             | `g.degrees.show()`                             |
| `g.filterEdges(...)`              | Lọc cạnh theo điều kiện                               | `g.filterEdges("relationship = 'follows'")`    |
| `g.pageRank()`                    | Xếp hạng tầm quan trọng (PageRank)                    | `g.pageRank(resetProbability=0.15, maxIter=5)` |
| `g.connectedComponents()`         | Tìm các thành phần liên thông                         | `g.connectedComponents().show()`               |
| `g.stronglyConnectedComponents()` | Thành phần liên thông mạnh                            | `g.stronglyConnectedComponents(maxIter=10)`    |
| `g.shortestPaths(...)`            | Tính đường đi ngắn nhất                               | `g.shortestPaths(landmarks=["1", "3"])`        |
| `g.triangleCount()`               | Đếm số tam giác qua mỗi đỉnh                          | `g.triangleCount().show()`                     |
| `g.find("...")`                   | Mẫu motif tìm kiếm (graph pattern matching)           | `g.find("(a)-[e]->(b); (b)-[e2]->(c)")`        |
| `.checkpoint()`                   | Lưu trạng thái trung gian cho các thuật toán phức tạp | `g.checkpoint()`                               |
# 📘 Ví dụ cụ thể
1. Tạo đồ thị nhỏ
```bash
from graphframes import GraphFrame
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()

# Vertices
vertices = spark.createDataFrame([
    ("1", "Alice"),
    ("2", "Bob"),
    ("3", "Charlie"),
    ("4", "David")
], ["id", "name"])

# Edges
edges = spark.createDataFrame([
    ("1", "2", "follows"),
    ("2", "3", "follows"),
    ("3", "1", "follows"),
    ("4", "1", "follows")
], ["src", "dst", "relationship"])

g = GraphFrame(vertices, edges)
```
2. Một số thao tác
```bash
g.vertices.show()
g.edges.show()
g.inDegrees.show()
g.outDegrees.show()
```
3. PageRank
```bash
pr = g.pageRank(resetProbability=0.15, maxIter=5)
pr.vertices.select("id", "pagerank").show()
```
4. Connected Components
```bash
cc = g.connectedComponents()
cc.select("id", "component").show()
```
5. Motif Finding
```bash
motifs = g.find("(a)-[e]->(b); (b)-[e2]->(c)")
motifs.select("a.name", "b.name", "c.name").show()
```












