# Large Scale Data Processing: Project 3 - 
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

3. **(3 points)**  
a. Run `LubyMIS` on `twitter_original_edges.csv` in GCP with 3x4 cores. Report the number of iterations, running time, and remaining active vertices (i.e. vertices whose status has yet to be determined) at the end of **each iteration**. You may need to include additional print statements in `LubyMIS` in order to acquire this information. Finally, verify your outputs with `verifyMIS`.  
b. Run `LubyMIS` on `twitter_original_edges.csv` with 4x2 cores and then 2x2 cores. Compare the running times between the 3 jobs with varying core specifications that you submitted in **3a** and **3b**.

## Submission via GitHub
Delete your project's current **README.md** file (the one you're reading right now) and include your report as a new **README.md** file in the project root directory. Have no fear—the README with the project description is always available for reading in the template repository you created your repository from. For more information on READMEs, feel free to visit [this page](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/about-readmes) in the GitHub Docs. You'll be writing in [GitHub Flavored Markdown](https://guides.github.com/features/mastering-markdown). Be sure that your repository is up to date and you have pushed all changes you've made to the project's code. When you're ready to submit, simply provide the link to your repository in the Canvas assignment's submission.

## You must do the following to receive full credit:
1. Create your report in the ``README.md`` and push it to your repo.
2. In the report, you must include your (and your partner's) full name in addition to any collaborators.
3. Submit a link to your repo in the Canvas assignment.

## Late submission penalties
Beginning with the minute after the deadline, your submission will be docked a full letter grade (10%) for every 
day that it is late. For example, if the assignment is due at 11:59 PM EST on Friday and you submit at 3:00 AM EST on Sunday,
then you will be docked 20% and the maximum grade you could receive on that assignment is an 80%. 
Late penalties are calculated from the last commit in the Git log.
**If you make a commit more than 48 hours after the deadline, you will receive a 0.**
