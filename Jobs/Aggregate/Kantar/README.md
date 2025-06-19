# Kantar Import Job

## Overview

This job handles the import of Kantar reports, processing them and storing them in S3.

## Supported Report Types

* Survey
* Category
* SubCategory
* Question

## Report info

The **GenerateCategoriesReport()** method generates a root report file (eg: 57b03785-9cd3-4e4f-b613-27f93fc68322-**argtgieng-2022r1_p-category-root**-0.json) for each survey.

The **GenerateChildCategoriesReport()** goes inside all categories existing in the survey (one by one) to generate a new report. If category type is Question it generates a question report (eg: 57b03785-9cd3-4e4f-b613-27f93fc68322-**argtgieng-2022r1_p-question-q_ar2021r12_2022r1_cb_pw_cl_111**-0.json) 
If category type is another category (SubCategory) it generates a detailed category report (eg: 57b03785-9cd3-4e4f-b613-27f93fc68322-**argtgieng-2022r1_p-category-h_ar2021r12_2022r1_cb_pw_cl_51111**-0.json).

So it's like **GenerateCategoriesReport()** is a master report and **GenerateChildCategoriesReport()** is detailed/node.