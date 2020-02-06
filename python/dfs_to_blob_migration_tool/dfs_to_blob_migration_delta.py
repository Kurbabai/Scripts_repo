# import libraries
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json
import os
import sys
from itertools import (takewhile, repeat)
import time

# Declare variables
company_name = "igloo"
environment_code = "p"
country_code = input("Please enter country code: ")
tenant_name = input("Please enter tenant name: ")
original_file = input("Please input delta file: ")
connect_str_from_passwordstate = input("Please enter the connection string from PasswordState: ")

dest_file = environment_code.upper() + country_code.upper() + tenant_name.upper() + "_" + time.strftime(
    "%Y_%m_%d-%H%M%S") + "delta" + ".txt"
dfs_share = "\\\\iglooprod.global\\p-" + country_code + "-binaries\\" + tenant_name + "-data0"
account_name_prefix = company_name + environment_code + country_code + tenant_name + "binariessa"


def main():
    try:
        i = 0
        num_lines_with_multipage = 0
        # open the original_file and read content
        num_lines = rawincount(original_file) + 1
        print("Total files in original file : " + str(num_lines) + "\n")
        file_name_set = set()
        blob_name_set = set()
        f = open(original_file, "r")
        if f.mode == "r":
            # readlines reads the individual lines
            line = f.readlines()
            while i < num_lines:
                for x in line:
                    # Remove \n characters from the line
                    x = x.translate({ord('\n'): None})
                    # split line per "," character and create split file path list
                    split_file_path_list = x.split(",")
                    file_path = split_file_path_list[0]
                    files_amount_in_line = int(split_file_path_list[1])
                    if files_amount_in_line == 1:
                        # split line per "\" character and create split list
                        split_list = file_path.split("\\")
                        container_name = split_list[5]
                        account_name = account_name_prefix + str(container_hex(split_list[6][:2]))
                        if os.path.isfile(file_path):
                            file_name = split_list[6] + "/" + split_list[7] + "/" + split_list[8]
                        else:
                            file_name = split_list[6] + "/" + split_list[7] + "/" + split_list[8] + "/1"
                        file_path_url = "https://" + account_name + ".blob.core.windows.net/" + container_name + "/" + file_name
                        file_name_set.add(file_path_url)
                        # open destination file and put there updated line
                        w = open(dest_file, "a+")
                        w.write(file_path_url + "\n")
                        w.close()
                        num_lines_with_multipage += files_amount_in_line
                    else:
                        # split line per "\" character and create split list
                        split_list = file_path.split("\\")
                        container_name = split_list[5]
                        account_name = account_name_prefix + str(container_hex(split_list[6][:2]))
                        j = 1
                        while j <= files_amount_in_line:
                            file_name = split_list[6] + "/" + split_list[7] + "/" + split_list[8] + "/" + str(
                                j)
                            file_path_url = "https://" + account_name + ".blob.core.windows.net/" + container_name + "/" + file_name
                            file_name_set.add(file_path_url)
                            # open destination file and put there updated line
                            w = open(dest_file, "a+")
                            w.write(file_path_url + "\n")
                            w.close()
                            j += 1
                        num_lines_with_multipage += files_amount_in_line
                    progress(i, num_lines, status="Reading files from the original list")
                    i += 1
        f.close()
        account_num = 1
        print("\n" + "Total files in original file after counting multipage files : " + str(
            num_lines_with_multipage) + "\n")
        # Get the list of blobs from every container of every storage account
        while account_num <= 32:
            if account_num <= 9:
                account_name = account_name_prefix + "0" + str(account_num)
            else:
                account_name = account_name_prefix + str(account_num)
            for blob_name_line in azure_blob_file_list(account_name, azure_connection_string(account_name,
                                                                                             connect_str_from_passwordstate)):
                if blob_name_line is not None:
                    blob_name_set.add(blob_name_line)
            account_num += 1
        # Determining the list of files need to be added and deleted to / from blobs
        #set_to_remove = blob_name_set - file_name_set
        set_to_add = file_name_set - blob_name_set
        print("\n" + "Total files in File name set: " + str(len(file_name_set)) + "\n")
        print("\n" + "Total files in storage accounts: " + str(len(blob_name_set)) + "\n")
        # Uploading missing files to the blob
        blob_added_count = 0
        j = 0
        for blob in set_to_add:
            # split line per "/" character and create split list
            blob_split_list = blob.split("/")
            account_url = blob_split_list[2]
            account_split_list = account_url.split(".")
            account_name = account_split_list[0]
            container_name = blob_split_list[3]
            if len(blob_split_list) == 8:
                blob_name = blob_split_list[4] + "/" + blob_split_list[5] + "/" + blob_split_list[6] + "/" + \
                            blob_split_list[7]
                source_filename = dfs_share + "\\" + container_name + "\\" + blob_split_list[4] + "\\" + \
                                  blob_split_list[5] + "\\" + blob_split_list[6] + "\\" + blob_split_list[7]
            else:
                blob_name = blob_split_list[4] + "/" + blob_split_list[5] + "/" + blob_split_list[6]
                source_filename = dfs_share + "\\" + container_name + "\\" + blob_split_list[4] + "\\" + \
                                  blob_split_list[5] + "\\" + blob_split_list[6]
            blob_client = BlobClient.from_connection_string(
                conn_str=azure_connection_string(account_name, connect_str_from_passwordstate),
                container_name=container_name, blob_name=blob_name)
            if os.path.exists(source_filename):
                with open(source_filename, "rb") as data:
                    blob_client.upload_blob(data)
                blob_added_count += 1
            else:
                # open exceptions file and put there updated line
                exceptions_file = "Exceptions_" + country_code + tenant_name + "_" + time.strftime("%Y_%m_%d-%H%M%S") + ".txt"
                exceptions = open(exceptions_file, "a+")
                exceptions.write(source_filename + "\n")
                exceptions.close()
            progress(j, len(set_to_add), status="Uploading missing files to the blob")
            j += 1
        print("\n" + str(blob_added_count) + " files have been uploaded")
        # Deleting unused files from the blob
        #blob_deleted_count = 0
        #k = 0
        #for blob in set_to_remove:
            # split line per "/" character and create split list
            #blob_split_list = blob.split("/")
            #account_url = blob_split_list[2]
            #account_split_list = account_url.split(".")
            #account_name = account_split_list[0]
            #container_name = blob_split_list[3]
            #if len(blob_split_list) == 8:
                #blob_name = blob_split_list[4] + "/" + blob_split_list[5] + "/" + blob_split_list[6] + "/" + \
                            #blob_split_list[7]
            #else:
                #blob_name = blob_split_list[4] + "/" + blob_split_list[5] + "/" + blob_split_list[6]
            #blob_client = BlobClient.from_connection_string(
                #conn_str=azure_connection_string(account_name, connect_str_from_passwordstate),
                #container_name=container_name, blob_name=blob_name)
            #blob_client.delete_blob()
            #blob_deleted_count += 1
            #progress(k, len(set_to_remove), status="Removing unused files from the blob")
            #k += 1
        #print("\n" + str(blob_deleted_count) + " files have been deleted")
        #if blob_deleted_count == 0 and blob_added_count == 0:
            #print("\n Congratulations! Your Storage accounts and DFS list fully synchronized!")

    except Exception as ex:
        print('Exception in main:')
        print(ex)


def azure_blob_file_list(account_name, connect_str):
    try:
        # Create the BlobServiceClient object which will be used to get a container client
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # List all containers
        all_containers = blob_service_client.list_containers(include_metadata=True)

        blob_list_in_container = []
        for container in all_containers:
            # Get container client
            container_client = blob_service_client.get_container_client(container)
            for blob in container_client.list_blobs():
                # List the blobs in the container
                blob_file = "https://" + account_name + ".blob.core.windows.net/" + container.name + "/" + blob.name
                blob_list_in_container.append(blob_file)
        return blob_list_in_container

    except Exception as ex:
        print('Exception in function azure_blob_file_list:')
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


def rawincount(filename):
    try:
        f = open(filename, 'rb')
        bufgen = takewhile(lambda x: x, (f.raw.read(1024 * 1024) for _ in repeat(None)))
        return sum(buf.count(b'\n') for buf in bufgen)
    except Exception as ex:
        print('Exception in function rawincount:')
        print(ex)


if __name__ == "__main__":
    main()
