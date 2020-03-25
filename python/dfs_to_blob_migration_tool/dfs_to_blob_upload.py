# import libraries
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json
import os
import sys
import time

# Declare variables

company_name = "igloo"
environment_code = "p"
country_code = input("Please enter country code: ")
tenant_name = input("Please enter tenant name: ")
original_file = input("Please input source file: ")
connect_str_from_passwordstate = input("Please enter the connection string from PasswordState: ")
missing_dest_file = original_file + "_missing_" + time.strftime(
    "%Y_%m_%d") + ".txt"
dest_file = environment_code.upper() + country_code.upper() + tenant_name.upper() + "_" + time.strftime(
    "%Y_%m_%d-%H%M%S") + "_delta" + ".txt"
#dfs_share = "\\\\iglooprod.global\\p-" + country_code + "-binaries\\" + tenant_name + "-data0"
account_name_prefix = company_name + environment_code + country_code + tenant_name + "binariessa"

def main():
    try:
        uploading_progress = 0
        num_lines_with_multipage = 0
        blob_added_count = 0
        # open the original_file and read content
        num_lines = rawcount(original_file)
        print("Total lines in the original file : " + str(num_lines) + "\n")
        print("\n Job started at " + time.strftime("%Y_%m_%d-%H%M%S") + "\n")
        f = open(original_file, "r")
        if f.mode == "r":
            # readlines reads the individual lines
            line = f.readlines()
            # while i < num_lines:
            for x in line:
                # Remove \n characters from the line
                x = x.translate({ord('\n'): None})
                # split line per "," character and create split file path list
                split_file_path_list = x.split(",")
                file_path = split_file_path_list[0]
                files_amount_in_line = int(split_file_path_list[1])
                split_list = file_path.split("\\")
                container_name = split_list[5]
                account_name = account_name_prefix + str(container_hex(split_list[6][:2]))
                if files_amount_in_line == 1:
                    if os.path.isdir(file_path):
                        file_name = file_path + "\\1"
                        if os.path.isfile(file_name):
                            dfs_path = file_name
                            blob_name = convert_dfs_path_to_url(dfs_path)
                            is_uploaded = file_upload_to_blob(account_name, container_name, blob_name, dfs_path)
                            if is_uploaded:
                                blob_added_count += 1
                        else:
                            w = open(missing_dest_file, "a+")
                            w.write(file_name + "\n")
                            w.close()
                    elif os.path.isfile(file_path):
                        dfs_path = file_path
                        blob_name = convert_dfs_path_to_url(dfs_path)
                        is_uploaded = file_upload_to_blob(account_name, container_name, blob_name, dfs_path)
                        if is_uploaded:
                            blob_added_count += 1
                    else:
                        w = open(missing_dest_file, "a+")
                        w.write(file_path + "\n")
                        w.close()
                else:
                    j = 1
                    while j <= files_amount_in_line:
                        file_name = file_path + "\\" + str(j)
                        if os.path.isfile(file_name):
                            dfs_path = file_name
                            blob_name = convert_dfs_path_to_url(dfs_path)
                            is_uploaded = file_upload_to_blob(account_name, container_name, blob_name, dfs_path)
                            if is_uploaded:
                                blob_added_count += 1
                        else:
                            w = open(missing_dest_file, "a+")
                            w.write(file_name + "\n")
                            w.close()
                        j += 1
                progress(uploading_progress, num_lines, status="Uploading files from the original list")
                uploading_progress += 1
                num_lines_with_multipage += files_amount_in_line
        f.close()
        print("\n" + "Total files in original file after counting multipage files : " + str(
            num_lines_with_multipage) + "\n")
        print("\n" + str(blob_added_count) + " files have been uploaded. Job finished at " + time.strftime("%Y_%m_%d-%H%M%S") + "\n")

    except Exception as ex:
        print('Exception in main:')
        print(ex)


def convert_dfs_path_to_url(dfs_path):
    try:
        split_dfs_path = dfs_path.split("\\")
        container_name = split_dfs_path[5]
        account_name = account_name_prefix + str(container_hex(split_dfs_path[6][:2]))
        if len(split_dfs_path) == 10:
            url_path = "https://" + account_name + ".blob.core.windows.net/" + container_name + "/" + split_dfs_path[6] + "/" + split_dfs_path[7] + "/" + split_dfs_path[8] + "/" + split_dfs_path[9]
            blob_name = split_dfs_path[6] + "/" + split_dfs_path[7] + "/" + split_dfs_path[8] + "/" + split_dfs_path[9]
        elif len(split_dfs_path) == 9:
            url_path = "https://" + account_name + ".blob.core.windows.net/" + container_name + "/" + split_dfs_path[6] + "/" + split_dfs_path[7] + "/" + split_dfs_path[8]
            blob_name = split_dfs_path[6] + "/" + split_dfs_path[7] + "/" + split_dfs_path[8]
        else:
            Print('DFS path has incorrect amount of subfolders')
        return blob_name

    except Exception as ex:
        print('Exception in convert_dfs_path_to_url:')
        print(ex)


def file_upload_to_blob(account_name, container_name, blob_name, dfs_path):
    try:
        blob_client = BlobClient.from_connection_string(conn_str=azure_connection_string(account_name, connect_str_from_passwordstate), container_name=container_name, blob_name=blob_name)
        blob_client.upload_blob(dfs_path)
        return True
    except Exception as ex:
        if isinstance(ex, ResourceExistsError):
            pass
        else:
             print('Exception in file_upload_to_blob:')
             print(ex)


def azure_connection_string(account_name, connect_str_from_passwordstate):
    try:
        # Remove backslash characters from PasswordState string
        str = connect_str_from_passwordstate.translate({ord('\\'): None})

        # Remove first and last quotes from the string
        json_str = str[1:-1]

        # Using json.loads()
        # Convert dictionary string to dictionary
        my_dict = json.loads(json_str)

        # Finding connection string in the dictionary using account name key"
        connect_str = my_dict.get(account_name + ".blob.core.windows.net")

        # return connection string for specific storage account
        return connect_str

    except Exception as ex:
        print('Exception in function azure_connection_string:')
        print(ex)


def container_hex(two_letters):
    try:
        sa_list_01 = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0a", "0b", "0c", "0d", "0e", "0f"]
        sa_list_02 = ["1", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "1a", "1b", "1c", "1d", "1e",
                      "1f"]
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

        if two_letters in sa_list_01:
            sa_number = "01"
        elif two_letters in sa_list_02:
            sa_number = "02"
        elif two_letters in sa_list_03:
            sa_number = "03"
        elif two_letters in sa_list_04:
            sa_number = "04"
        elif two_letters in sa_list_05:
            sa_number = "05"
        elif two_letters in sa_list_06:
            sa_number = "06"
        elif two_letters in sa_list_07:
            sa_number = "07"
        elif two_letters in sa_list_08:
            sa_number = "08"
        elif two_letters in sa_list_09:
            sa_number = "09"
        elif two_letters in sa_list_10:
            sa_number = "10"
        elif two_letters in sa_list_11:
            sa_number = "11"
        elif two_letters in sa_list_12:
            sa_number = "12"
        elif two_letters in sa_list_13:
            sa_number = "13"
        elif two_letters in sa_list_14:
            sa_number = "14"
        elif two_letters in sa_list_15:
            sa_number = "15"
        elif two_letters in sa_list_16:
            sa_number = "16"
        else:
            sa_number = "not_defined"
        return sa_number
    except Exception as ex:
        print('Exception in function container_hex:')
        print(ex)


def progress(count, total, status=''):
    try:
        bar_len = 60
        filled_len = int(round(bar_len * count / float(total)))
        percents = round(100.0 * count / float(total), 1)
        bar = '=' * filled_len + '-' * (bar_len - filled_len)
        sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
        sys.stdout.flush()
    except Exception as ex:
        print('Exception in function progress:')
        print(ex)


def rawcount(filename):
    try:
        f = open(filename, 'rb')
        lines = 0
        buf_size = 1024 * 1024
        read_f = f.raw.read
        buf = read_f(buf_size)
        while buf:
            lines += buf.count(b'\n')
            buf = read_f(buf_size)
        return lines
    except Exception as ex:
        print('Exception in function rawcount:')
        print(ex)


if __name__ == "__main__":
    main()
