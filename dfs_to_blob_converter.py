# import libraries
import os
import shutil
import sys
import time

# Declare variables
original_file = "list.txt"
environment_code = "p"
country_code = "us"
tenant_name = "afa"


def main():
    i = 0
    # create empty list for azcopy commands
    commands_list = []
    # check if base folder exist , delete and create new
    base_folder = "blob_migration"
    # Delete all contents of a directory using shutil.rmtree() and  handle exceptions
    try:
       shutil.rmtree(base_folder)
       print('Folder ' + base_folder + ' has been deleted')
    except:
       print('Error while deleting blob_migration directory')
    #Create new base folder
    base_folder = create_folder("blob_migration")
    # open the original_file and read content
    f = open(original_file, "r")
    num_lines = sum(1 for line in open(original_file))
    if f.mode == "r":
        # readlines reads the individual lines
        line = f.readlines()
        for x in line:
            while i < num_lines:
                progress(i, num_lines, status="Reading files")
                # split line per "\" character and create splitted list
                splitted_list = x.split("\\")
                command_string = ".\\azcopy.exe copy \"\\\\" + splitted_list[2] + "\\" + splitted_list[3] + "\\" + \
                                 splitted_list[4] + "\\" + splitted_list[5] + "\\" + splitted_list[
                                     6] + "\\\"" + " \"https://igloo" + environment_code + country_code + tenant_name + "binariessa" + container_hex(
                    splitted_list[6][:2]) + ".blob.core.windows.net/" + splitted_list[
                                     5] + "/\" --list-of-files \"C:\\temp\\blob_migration\\" + country_code.upper() + tenant_name.upper() + "\\" + \
                                 splitted_list[5] + "\\" + splitted_list[6] + ".txt\""

                if command_string not in commands_list:
                    commands_list.append(command_string)

                dest_folder = create_folder("blob_migration\\" + splitted_list[5])
                dest_file = "blob_migration\\" + splitted_list[5] + "\\" + splitted_list[6] + ".txt"
                w = open(dest_file, "a+")
                w.write(splitted_list[7] + "/" + splitted_list[8])
                w.close()
                i +=1
    for command in commands_list:
        print(command)


def container_hex(item):
    sa_list_01 = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0a", "0b", "0c", "0d", "0e", "0f"]
    sa_list_02 = ["1", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "1a", "1b", "1c", "1d", "1e", "1f"]
    sa_list_03 = ["20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "2a", "2b", "2c", "2d", "2e", "2f"]
    sa_list_04 = ["30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "3a", "3b", "3c", "3d", "3e", "3f"]
    sa_list_05 = ["40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "4a", "4b", "4c", "4d", "4e", "4f"]
    sa_list_06 = ["50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "5a", "5b", "5c", "5d", "5e", "5f"]
    sa_list_07 = ["60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "6a", "6b", "6c", "6d", "6e", "6f"]
    sa_list_08 = ["70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "7a", "7b", "7c", "7d", "7e", "7f"]
    sa_list_09 = ["80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "8a", "8b", "8c", "8d", "8e", "8f"]
    sa_list_10 = ["90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "9a", "9b", "9c", "9d", "9e", "9f"]
    sa_list_11 = ["a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "aa", "ab", "ac", "ad", "ae", "af"]
    sa_list_12 = ["b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9", "ba", "bb", "bc", "bd", "be", "bf"]
    sa_list_13 = ["c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "ca", "cb", "cc", "cd", "ce", "cf"]
    sa_list_14 = ["d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "da", "db", "dc", "dd", "de", "df"]
    sa_list_15 = ["e0", "e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8", "e9", "ea", "eb", "ec", "ed", "ee", "ef"]
    sa_list_16 = ["f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "fa", "fb", "fc", "fd", "fe", "ff"]

    if item in sa_list_01:
        sa_number = "01"
    elif item in sa_list_02:
        sa_number = "02"
    elif item in sa_list_03:
        sa_number = "03"
    elif item in sa_list_04:
        sa_number = "04"
    elif item in sa_list_05:
        sa_number = "05"
    elif item in sa_list_06:
        sa_number = "06"
    elif item in sa_list_07:
        sa_number = "07"
    elif item in sa_list_08:
        sa_number = "08"
    elif item in sa_list_09:
        sa_number = "09"
    elif item in sa_list_10:
        sa_number = "10"
    elif item in sa_list_11:
        sa_number = "11"
    elif item in sa_list_12:
        sa_number = "12"
    elif item in sa_list_13:
        sa_number = "13"
    elif item in sa_list_14:
        sa_number = "14"
    elif item in sa_list_15:
        sa_number = "15"
    elif item in sa_list_16:
        sa_number = "16"
    else:
        sa_number = "not_defined"
    return sa_number


def create_folder(folder_name):
    if not os.path.exists(folder_name):
        os.mkdir(folder_name)
        print("Directory " + folder_name +  " created \n\r")

def progress(count, total, status=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))
    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)
    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()

if __name__ == "__main__":
    main()
