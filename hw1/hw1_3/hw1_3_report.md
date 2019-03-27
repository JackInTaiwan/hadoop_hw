# Dig Data Management Assignment 1

## Task3 - Comparison between bulk file as big data and piles of ones as small peices of data

Here, we split whole task into two main functions. One (*Part I*) is implemented for uploading bulk file to HDFS where we fusion all images into one bulk file, while the other (*Part II*) is implemented for reading image files from HDFS with corresponding index file we create and maintain. 


### Part I : Uploading Bulk File
* First of all, the main source code is shown below:
![be58c85e.png](:storage/462de0fe-8137-4b53-b5f9-fe51de95485a/a3c410a1.png)
<br>
* Read the local origin files (around 1.7 GB) and append the file bytes `output_file` together.
![b5635108.png](:storage/462de0fe-8137-4b53-b5f9-fe51de95485a/b5635108.png)
<br>
* In the meanwhile, record the offset each single image file holds in the bulk bytes variable `output_file` in variable `file_index_list` in favor of variable `file_index_count`.
![d56c2b58.png](:storage/462de0fe-8137-4b53-b5f9-fe51de95485a/d56c2b58.png)
<br>
* Done with the reading task, we produce index file `index.txt` to help our reading task on HDFS bulk file in the future.
![9689bdcd.png](:storage/462de0fe-8137-4b53-b5f9-fe51de95485a/9689bdcd.png)
Note that the index is designed to contain two row with delimiter of `,`, one of which is for offsets, the other of which is for file names. `index.txt` is shown below:
![78463e9e.png](:storage/462de0fe-8137-4b53-b5f9-fe51de95485a/78463e9e.png)
<br>
* At the end, upload the bulk file via HDFS API on Python (we use library `hdfs` here).
![360e5c7e.png](:storage/462de0fe-8137-4b53-b5f9-fe51de95485a/360e5c7e.png)



<br><br>
### Part II : Reading Bulk File
* First of all, the main source code is shown below:
![653e8fba.png](:storage/462de0fe-8137-4b53-b5f9-fe51de95485a/653e8fba.png)
<br>
* Read our index file `index.txt` and trasform it into two search lists. One is for file name, and the other is for corresponding offset. Read the whole bulk file into memory, then look up and extract the target image bytes based on its offset obtained by searching its filename in list `filename_list`.
![2965297a.png](:storage/462de0fe-8137-4b53-b5f9-fe51de95485a/2965297a.png)
<br>
* Do a try-catch in the case of absence of the target file name.
![2b8733b8.png](:storage/462de0fe-8137-4b53-b5f9-fe51de95485a/2b8733b8.png)

<br><br>
### Analysis : Advantages of Bulk File
* Equal consumed logical memory
| | Bulk file | Origin Files |
| :- | :-: | :-: |
| Total size (B) | 1761627280 | 1761627280 |


![e087d8a5.png](:storage/462de0fe-8137-4b53-b5f9-fe51de95485a/e087d8a5.png)

![d075a6ec.png](:storage/462de0fe-8137-4b53-b5f9-fe51de95485a/d075a6ec.png)

* Less number of occupied blocks
| | Bulk file | Origin Files |
| :- | :-: | :-: |
| Total blocks | 14 | 140 |

![bc316c8d.png](:storage/462de0fe-8137-4b53-b5f9-fe51de95485a/bc316c8d.png)

![e931ad31.png](:storage/462de0fe-8137-4b53-b5f9-fe51de95485a/e931ad31.png)

* Higher speed of saving and uplaoding
| | Bulk file | Original Files |
| :- | :-: | :-: |
| Creating bulk file (s) | 11.21 | - |
| Uploading file (s) | 24.41 | 37.69 |
| Total (s) | 35.62 | 37.69 |

The totally consumed time is quite the same, while uploading one big bulk file is faster than uploading piles of small files even if they're the same in total size.
Note that we upload original small files via terminal which is based on Java while we upload the bulk file via Python, which indicates that the totally consumed time in the case of bulk file is faster than the one shown in the table.

* Higher speed when reading through all image files
| | Bulk file | Original Files |
| :- | :-: | :-: |
| Total time (s) | 8.70 | 10.82 |

We read through all 140 image files in two cases. One case is that we fetch the bulk file back and cut them into original image files according to the index file. The other is that we iteratively call reading API to get all image files one by one.

![939b7c2b.png](:storage/462de0fe-8137-4b53-b5f9-fe51de95485a/939b7c2b.png)














