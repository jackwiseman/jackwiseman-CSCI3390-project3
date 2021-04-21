# Large Scale Data Processing: Project 3
**Nick Bearup, Jacob Kennedy, Jack Wiseman**

## Question 1 - verifyMIS()

|        Graph file       |           MIS file           | Is an MIS? |
| ----------------------- | ---------------------------- | ---------- |
| small_edges.csv         | small_edges_MIS.csv          | Yes        |
| small_edges.csv         | small_edges_non_MIS.csv      | No         |
| line_100_edges.csv      | line_100_MIS_test_1.csv      | Yes        |
| line_100_edges.csv      | line_100_MIS_test_2.csv      | No         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_1.csv | Yes        |
| twitter_10000_edges.csv | twitter_10000_MIS_test_2.csv | Yes        |

## Question 2 - LubyMIS()

|        Graph file       | Iterations | Runtime |
| ----------------------- | ---------- | ------- |
| small_edges.csv         | 1	       | 2s      |
| line_100_edges.csv      | 3          | 2s      |
| twitter_100_edges.csv   | 2          | 2s      |
| twitter_1000_edges.csv  | 2          | 2s      |
| twitter_10000_edges.csv | 3          | 3s      |

## Question 3 - Google Cloud

##### 3x4 Cores

| Iteration | Remaining Vertices |
| --------- | ------------------ |
| 1         | 6868625		 |
| 2	    | 34369		 |
| 3	    | 423		 |
| 4         | 5 		 |
| 5  	    | 0			 |

Runtime - 202s

##### 4x2 Cores

| Iteration | Remaining Vertices |
| --------- | ------------------ |
| 1         | 6916474		 |
| 2	    | 39416		 |
| 3	    | 430		 |
| 4         | 4 		 |
| 5  	    | 0			 |

Runtime - 156s

##### 2x2 Cores

| Iteration | Remaining Vertices |
| --------- | ------------------ |
| 1         | 6703981		 |
| 2	    | 34179		 |
| 3	    | 495		 |
| 4         | 4 		 |
| 5  	    | 0			 |

Runtime - 281s

