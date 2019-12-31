DFS to Blob Converter 

Converting list of files  pointing to DFS storage to the list of azcopy commands:

original  list : 

\\iglooprod.global\p-us-binaries\afa-data0\binaries\04868695-2c50-4e50-b044-d6bb8c79cd04\00045b8e-bed9-45b3-86a1-95ef826d21a3\00045b8e-bed9-45b3-86a1-95ef826d21a3.jpg

\\iglooprod.global\p-us-binaries\afa-data0\binaries\1\attachments\00T85TAXCB.bin

\\iglooprod.global\p-us-binaries\afa-data0\binaries\1000079990\010bd59b-8977-4ac6-b5ab-54ec879563a8\010bd59b-8977-4ac6-b5ab-54ec879563a8crlarge.png


Commands list :

.\azcopy.exe copy "\\iglooprod.global\p-us-binaries\afa-data0\binaries\04868695-2c50-4e50-b044-d6bb8c79cd04\" "https://igloopusafabinariessa01.blob.core.windows.net/binaries/" --list-of-files "C:\temp\blob_migration\USAFA\binaries\04868695-2c50-4e50-b044-d6bb8c79cd04.txt"
.\azcopy.exe copy "\\iglooprod.global\p-us-binaries\afa-data0\binaries\1\" "https://igloopusafabinariessa02.blob.core.windows.net/binaries/" --list-of-files "C:\temp\blob_migration\USAFA\binaries\1.txt"
.\azcopy.exe copy "\\iglooprod.global\p-us-binaries\afa-data0\binaries\1000079990\" "https://igloopusafabinariessa02.blob.core.windows.net/binaries/" --list-of-files "C:\temp\blob_migration\USAFA\binaries\1000079990.txt"
