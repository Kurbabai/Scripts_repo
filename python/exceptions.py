import os
source_folder = "C:\\Temp\\blob_migration\\CAJFH\\exceptions"
file_name_set = set()
for file in os.listdir(source_folder):
    #print(file)
    filepath = os.path.join(source_folder, file)
    f = open(filepath, 'r')
    line = f.readlines()
    for line in f.readlines():
        file_name_set.add(line)
    f.close()
for file_name in file_name_set:
    w = open("C:\\Temp\\blob_migration\\CAJFH\\exceptions\\exceptions.txt", "a+")
    w.write(file_name + "\n")
    print(file_name)
    w.close()

