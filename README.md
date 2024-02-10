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
 #### The Pages are stored in dictionaries. There are two dictionary members in a table instance. base_page_dict is responsible for storing base pages, while tail_page_dict is responsible for storing tail pages. They key of the page_dict is the column_index of this column of record, and the value of the dictionary is a list which contains the instaces of corresponding pages. Example: 0:[Page0, Page1], 1:[Page0, Page1, Page2]...
 
 #### When the table is created, the table will 1 base page and 1 tail page for every column of the table. 

#### We periodically merge the base page and a tail page of a record, so we do not need to allocate new tail page for an existed record. However, it is possible that we insert to many records to our base page, then will will create # of num_col pages and append them to appropriate nplaces in the 2-D page list.

### Index
#### We use b-tree to implement the index of our database. In all the base pages, the primary key should be unique. However, in the tail page, the primary key may be repeated. So we need to create 2 b-tree for each column. One is for base pages, which uses the primary key as the key of the tree. The other one is for tail pages, which uses RID as the key of the tree. The value of both trees is the address of the data stored in a list. 

#### The address list contains three integers looks like this : [a, b, c], where a is the index of page range, b is the index of page in this page range, c is the index of record in this page.

#### For example, [0, 2, 5] means this data is in the [1st page range, 3rd page, 6th position in the bytearray]

### Get and modify data

#### If you know the address of a data, how do you quickly get the data and modify it? You can use the internal query in query.py. 

#### get_page_value() returns the data (read only)

#### modify_page_value() is used to modify the data
