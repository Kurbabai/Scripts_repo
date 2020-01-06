# import libraries
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json
import sys
from itertools import (takewhile, repeat)
import time

# Declare variables
company_name = "igloo"
environment_code = "p"
country_code = "us"
tenant_name = "afa"
connect_str_from_passwordstate = input("Please enter connection string from PasswordState: ")
original_file = "delta_list.txt"
dest_file = environment_code.upper() + country_code.upper() + tenant_name.upper() + "-" + time.strftime(
    "%Y_%m_%d-%H%M%S") + ".txt"
dfs_share = "rnour-lp"
account_name_prefix = company_name + environment_code + country_code + tenant_name + "binariessa"


def main():
    try:
        i = j = 0

        # open the original_file and read content
        num_lines = rawincount(original_file) + 1
        print("Total files in original file : " + str(num_lines))

        file_name_set = set()
        blob_name_set = set()
        f = open(original_file, "r")
        if f.mode == "r":
            # readlines reads the individual lines
            line = f.readlines()
            while i < num_lines:
                for x in line:
                    print(x)
                    # Remove \n characters from the line
                    x = x.translate({ord('\n'): None})
                    # split line per "\" character and create splitted list
                    splitted_list = x.split("\\")
                    container_name = splitted_list[5]
                    account_name = account_name_prefix + str(container_hex(splitted_list[6][:2]))
                    file_name = splitted_list[6] + "/" + splitted_list[7] + "/" + splitted_list[8]
                    file_path_url = "https://" + account_name + ".blob.core.windows.net/" + container_name + "/" + file_name
                    file_name_set.add(file_path_url)
                    # open destination file and put there updated line
                    w = open(dest_file, "a+")
                    w.write(file_path_url + "\n")
                    w.close()

                    print(file_path_url)
                    print("Connection string for " + account_name + ": " + str(
                        azure_connection_string(account_name, connect_str_from_passwordstate)))
                    # Adding the line to original_file_set
                    # original_file_set.add(x)

                    progress(i, num_lines, status="Reading files")
                    i += 1
        f.close()

        container_name = "test"
        account_num = 1
        while account_num <= 32:
            if account_num < 9:
                account_name = account_name_prefix + "0" + str(account_num)
            else:
                account_name = account_name_prefix + str(account_num)
            for blob in azure_blob_file_list(account_name,
                                             azure_connection_string(account_name, connect_str_from_passwordstate)):
                blob_name_set.add(
                    "https://" + account_name + ".blob.core.windows.net/" + container_name + "/" + blob.name)
            account_num += 1

        set_to_remove = blob_name_set - file_name_set
        set_to_add = file_name_set - blob_name_set
        print("Blob name set : " + str(blob_name_set))
        print(" File name set : " + str(file_name_set))
        # print("Set to remove :" + str(set_to_remove))
        print("Set to add :" + str(set_to_add))

    except Exception as ex:
        print('Exception in main:')
        print(ex)


# set_for_deletion = blob_name_set - original_file_set
# set_for_adding = original_file_set - blob_name_set
# print("Original file set : " + str(original_file_set))
# print("Set for deletion : " + str(set_for_deletion))
# print("Set for adding : " + str(set_for_adding))


def azure_blob_file_list(account_name, connect_str):
    try:
        # Create the BlobServiceClient object which will be used to get a container client
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # List all containers
        all_containers = blob_service_client.list_containers(include_metadata=True)
        for container in all_containers:
            # Get container client
            container_client = blob_service_client.get_container_client(container)
            # List the blobs in the container
            blob_list = container_client.list_blobs()
            # Return blob_list object
            return blob_list

            # print(account_name)
            # print(container['name'], container['metadata'])

        # if container_client:

        # else:
        # print("Container " + container_name + " is not exist on " + account_name)

    except Exception as ex:
        print('Exception in function azure_blob_file_list:')
        print(ex)


def azure_connection_string(account_name, connect_str_from_passwordstate):
    try:
        # Remove backslash characters from passwordstate string
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
