# import libraries
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json
import sys
from itertools import (takewhile, repeat)

# Declare variables
account_name = "igloopusafabinariessa01"
connect_str_from_passwordstate = '"{\\"igloopusafabinariessa01.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa01;AccountKey=BHjPBkOxmwHB+4Y2/TTzrZvOaNys0usgXVHM4YS6QCTC1qvWvek1itTkc4mcWmfwmSLQthr+RJHVkq2LjA83SA==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa02.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa02;AccountKey=G3093zL0DHB5JOwztVV5tcdFhDCOxAO5aop4Alv3uBOYo/x+XeDbQYInzZaCa0J+QV2Yh866GcYMNyq6FqruYg==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa03.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa03;AccountKey=qJj5jOYqiPsmG29+Ntgbu6+zPEXaiWvrp1Aeg7JA9zrKeAIK0dMlZCBMCvKAczY0LMDr6LIEEUew2tG7muN+ZQ==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa04.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa04;AccountKey=K80BcWQcIH6ZZxFuMiZR6O1/g7oJbSrEl3YPyBY60HRBCrL07MHAcJv1uYhX2ur2+Ruv0tvoVqVdCfzyDVg+8A==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa05.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa05;AccountKey=uTLYFKdnrjBRPDnbPYN7NNk4fiiR1xjE3MAW1KEVP73zTrJFNad4noDOEXoFz6qSDV5M5pKPAMGfNgvUgJhSNg==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa06.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa06;AccountKey=SwSA8zwZkBBuFmKIrKlPlH3LpQX83a+101BTiVE0rT7VGPxljNNo0L5XGPUO6PdAYmhjOpEkV29WsYSyxUcryg==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa07.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa07;AccountKey=OYLGgK34kpowruEPLC+pbxHb5yJnMvPCWsjjYvJKEXaQGSnupIhxy86QsyVtGYnEk4ZWyBxfy145+UV2BmYznQ==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa08.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa08;AccountKey=J7hxxsCwtsjzTVoasQD548pkyxsR7h6LJQLb77kgkoE0c2M8MpE6FmhxXBwcghnP5M1VJfNTkIlM1ZrEQyMqag==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa09.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa09;AccountKey=1P2DwJp/iyPCfh7RLPyY8bDHSW64l9p62Hxi6KXoj5loMqgQtFxEmpRwDnkN8wPSP8JXi4LuoctyG9jQ2sZCpA==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa10.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa10;AccountKey=lVymVFAsN526Rfb3e3S5LhVb2tTVM6iiA7DUfQs98jPxxn8p7LlsFEcxoBuXjwK8NHvdKgBa53rgQe2CUmHN3Q==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa11.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa11;AccountKey=xKy8Ud6xwudcpBd/3A2roPbIGiPkzQVVhNs7xAvAt/BxDpDoNlfjhepKZb/CzUISf1/W5qkorJQui7dW0HS6+A==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa12.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa12;AccountKey=yfh/dknKrJt2ATE5bNcvq/V5UW3lzFoHSfsSBc9JgSYvxYcOlEEpk74LLUENwpoS8uB1G9hQEBTkI/BrCiqBAA==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa13.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa13;AccountKey=oS/PBSGF624Cvw/3K7R85lChgwsTyNCv2B0BZdOBiwHKgns6LcCXRGeZxyS9sDRHTyDxhfM8GK2oUC7lSHLaaQ==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa14.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa14;AccountKey=zpAAe3h7yvjV0VLAu4R2spDHqHrG1xvzfVx5FVTU/UZE/3dQqdjm3QzlKYjDiIQU/x6x9OQ6rrYTBkNOXmfrag==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa15.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa15;AccountKey=ZXepeuMWPkQy/+DSsyrTDz4UKVfOl62h3jFCTbCUVFDMwMp/0JVyLZpxilQVKphNCs3oPmKkHKEQN3qgSDszww==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa16.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa16;AccountKey=mWcR8nKuGHjRnBo6Ux767sskr7b5y6B1f8KHiLmC5qZKkc9rJMabJT8FkX4hPnzQZKVPqQzGWYAtnL005QLaPw==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa17.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa17;AccountKey=OSOZw9DL5Z519/1BROfPy5eml6J/FoCCHuiX0CaoNzgNGUneb6/zsUYUEaYiBKxHhDv0rVJQDqQXTETVZK1ThQ==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa18.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa18;AccountKey=rfUO9f1C4q3bqegyGsUw8lLtWgO5w59hHDJV75cRqg+QxW9OsJNCgI4lGNv7Hkxy2SP4bJtFxoYWPHYsJxhhZA==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa19.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa19;AccountKey=31yyuEPSzIQfIC6+QVAB87hEyUjpWS6guCuwXHFvkONxbRm0+2HFkcQ+4uooyMUpcisx5jhnNHkS2XggcIsdtg==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa20.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa20;AccountKey=oqM6e7+de5KSRZtK5cnd20iZdS8H0cgPm6/ETdksAaAYxTDNQYY/cFYi5TE6/9tpahmuFIU0/sj/5Uu9kDJRyA==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa21.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa21;AccountKey=NkTx87yd3lyiJ/bC9b4SoJL3qqNDD1vYR/6P7hrPerOcqicya6OIas0HY+w8uJP5TT59na+LGnGBXdmosChZBA==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa22.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa22;AccountKey=VFm5AezO9o83LmqxNLR9/1WYC52bIfhGEs9PztN+GLYuUJalb+hpTXc4BZnQqQ3ATBGNf1kFldSomvytx3g5cw==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa23.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa23;AccountKey=LNTIQLYQAwOjW+D+MkwU64YJqgl4rn+kSqxqPzGnJ1asizveoAsz1g7AvnpXBWJ3zQ59W9CKqzYwZfYUxuTYvQ==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa24.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa24;AccountKey=AU8aflinEymvB8z7vOfbxX0JHatCiCtY0MQvuigMPA35h/JjiBYIl8z582p9uT+0sFk2zmw37z9I+0CnWt5j3A==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa25.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa25;AccountKey=Osjp6btpH0zNqKux4AnhpJMjkM4INIYV9Tmq+KFySIDfB190O/FyCDr5LfJglUkiwRE7ei/P8A6o05YP+C2v5w==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa26.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa26;AccountKey=0kbYJ+psIOtXVg9tIm8cR9V473nQ62U97cFSVlZU0NkOJkP5XeX4CoQCUz4+GfT8vyG+P5K3zMJm5VbT7kN5nw==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa27.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa27;AccountKey=FGkiYuQIopYU5N8AglqYoq+pDIEyCKg2Sr15QTvyNYZN/Iiw+sKfO8f83nUP4oUjEGMMNUqEIS4pZ9mU3liuLg==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa28.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa28;AccountKey=nHoDmMDfd0zvQrArCxn6XqJPSTt+yWilfWiaLnqY0l3IG5Wc0q6/waJevQw6PYs4UBmobAXWRMyHLTTGbYmgAQ==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa29.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa29;AccountKey=m4RdS0J0eZGOrj+tDLWDNBAjIPRsw0uBrCXdwRwh7AeIFS5voG7yGeDyAmtf8Ooi9rz3qshcaGwXOm5c74ggOg==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa30.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa30;AccountKey=pU5ITj565wXeWQZw/44oeB+OsAnHfYtJQseC+fJ+koddRzZmbb6kMNixHJg6TM+JNDeIrnRL+mK7dvgM7VejQA==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa31.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa31;AccountKey=sIQ5tBgvrH4zaPrThXWel6nUISexklLL+MZlCYi6fc7ZH1AESJL5cPvH2W4zS945vxSaBz3KjdOmhQwFYVcDrw==;EndpointSuffix=core.windows.net\\", \\"igloopusafabinariessa32.blob.core.windows.net\\":\\"DefaultEndpointsProtocol=https;AccountName=igloopusafabinariessa32;AccountKey=oZEnVpXnq+NZiggRD98JBOSdWPIJKkold7gBcbQm0EhgGkjyJfDTrc9FYlhp0NMUP6lS17Qv1cs0Qs78m2rJwQ==;EndpointSuffix=core.windows.net\\"}"'
container_name = "test"
original_file = "list.txt"


def main():
    try:
        print("Connection string for " + account_name + ": " + azure_connection_string(account_name, connect_str_from_passwordstate))

        blob_name_set = set()
        for blob in azure_blob_file_list(container_name, azure_connection_string(account_name, connect_str_from_passwordstate)):
            blob_name_set.add(blob.name)
        print("Blob name set : " + str(blob_name_set))

        i = 0

        # open the original_file and read content
        num_lines = rawincount(original_file) + 1

        print("Total files in original file : " + str(num_lines))

        original_file_set = set()
        f = open(original_file, "r")
        if f.mode == "r":
            # readlines reads the individual lines
            line = f.readlines()
            while i < num_lines:
                for x in line:
                    # Remove \n characters from the line
                    x = x.translate({ord('\n'): None})
                    # Adding the line to original_file_set
                    original_file_set.add(x)
                    progress(i, num_lines, status="Reading files")
                    i += 1

        set_for_deletion = original_file_set.intersection(blob_name_set)
        print("Original file set : " + str(original_file_set))
        print("Set for deletion : " + str(set_for_deletion))

    except Exception as ex:
        print('Exception:')
        print(ex)


def azure_blob_file_list(container_name, connect_str):
    try:
        # Create the BlobServiceClient object which will be used to get a container client
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Get container client
        container_client = blob_service_client.get_container_client(container_name)

        # List the blobs in the container
        blob_list = container_client.list_blobs()

        # Return blob_list object
        return (blob_list)

    except Exception as ex:
        print('Exception')
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
        return (connect_str)

    except Exception as ex:
        print('Exception:')
        print(ex)


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


def progress(count, total, status=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))
    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)
    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()


def rawincount(filename):
    f = open(filename, 'rb')
    bufgen = takewhile(lambda x: x, (f.raw.read(1024 * 1024) for _ in repeat(None)))
    return sum(buf.count(b'\n') for buf in bufgen)


if __name__ == "__main__":
    main()
