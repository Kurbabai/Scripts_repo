def main():
    try:
        file_name_set = set()
        blob_name_set = set()
        file_name_set.add("one")
        file_name_set.add("two")
        file_name_set.add("three")
        blob_name_set.add("one")
        blob_name_set.add("two")
        #blob_name_set.add("three")
        blob_name_set.add("four")
        blob_name_set.add("five")
        set_to_add = file_name_set - blob_name_set
        set_to_remove = blob_name_set - file_name_set
        print("\n" + "Total files in File name set: " + str(len(file_name_set)) + "\n")
        print("\n" + "Total files in storage accounts: " + str(len(blob_name_set)) + "\n")
        print("\n" + "File Name Set: " + str(file_name_set))
        print("\n" + "Blob Name Set: " + str(blob_name_set))
        print("\n" + "Set to Add: " + str(set_to_add))
        print("\n" + "Set to Remove: " + str(set_to_remove))
        print(type(file_name_set))
        print(type(blob_name_set))
        print(type(set_to_add))
        print(type(set_to_remove))
    except Exception as ex:
        print('Exception in main:')
        print(ex)


if __name__ == "__main__":
    main()
