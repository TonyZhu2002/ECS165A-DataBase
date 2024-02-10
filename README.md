# ECS165A Project

## Requirement:
* Python environment
* btree - [Documentation here](https://pypi.org/project/BTrees/#files)


## Progress:

* ### Milestone1:
    - [x] Create db 
    - [x] Create table
    - [x] Create page
    - [x] Use index to keep track of data
    - [x] Query: Insert
    - [ ] Query: Delete
    - [ ] Query: Update
    - [ ] Query: Select
    - [ ] Query: select_version
    - [ ] Query: sum
    - [ ] Query: sum_version
    - [ ] Query: increment
    - [x] Allocate
    - [ ] Merge

## Our design:
### Pages:
 #### We create another class called PageRange to store the Page instances. In every table, there are 1 base page dictionary and 1 tail page dictionary. When the table gets initialized, it automaticly creates 1 base page and 1 tail page for each column. Then table assigns appropriate number of pageranges for these pages. For each column, its pageranges are stored in a list. The key of the dictionary is the index of the column, where the value of the dictionary is the list of pageranges.
 

#### We will periodically merge the base page and a tail page of a record. However, sometimes a base page or a tail page will be full. The table will allocate more pages for the column and store these pages in appropriate pagerange.

### Index
#### We use b-tree to implement the index of our database. In all the base pages, the primary key should be unique. However, in the tail page, the primary key may be repeated. So we need to create 2 b-tree for each column. One is for base pages, which uses the primary key as the key of the tree. The other one is for tail pages, which uses RID as the key of the tree. The value of both trees is the address of the data stored in a list. 

#### The address list contains three integers looks like this : [a, b, c, d, e], where a is the indicator of base page(0 for base page and 1 for tail page). b is the column index for the data, c is the index of the page range in our page_range list, d is the index of the page inside our page range, and e is the index of the record in the page.

#### For example, [0, 2, 5, 6, 10] means this data is in the a base page, it is a data in the 2nd column. The data is stored in the 5th page range, 6th page, and 10th position of the page.

### Get and modify data

#### If you know the address of a data, how do you quickly get the data and modify it? You can use the internal query in query.py. 

#### get_page_value() returns the data (read only)

#### modify_page_value() is used to modify the data
