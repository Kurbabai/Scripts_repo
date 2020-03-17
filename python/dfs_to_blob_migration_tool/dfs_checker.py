# import libraries
import os
import sys
from itertools import (takewhile, repeat)
import time

# Declare variables
original_file = input("Please input source file: ")

missing_dest_file = original_file + "_missing_" + time.strftime(
    "%Y_%m_%d") + ".txt"
existing_dest_file = original_file + "_existing_" + time.strftime(
    "%Y_%m_%d") + ".txt"


def main():
    try:
        reading_progress = 0
        num_lines_with_multipage = 0
        # open the original_file and read content
        num_lines = rawincount(original_file) + 1
        print("Total files in original file : " + str(num_lines) + "\n")
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
                if files_amount_in_line == 1:
                    if os.path.isdir(file_path):
                        if os.path.isfile(file_path + "/1"):
                            w = open(existing_dest_file, "a+")
                            w.write(file_path + "/1" + "\n")
                            w.close()
                        else:
                            w = open(missing_dest_file, "a+")
                            w.write(file_path + "/1" + "\n")
                            w.close()
                    elif os.path.isfile(file_path):
                        w = open(existing_dest_file, "a+")
                        w.write(file_path + "\n")
                        w.close()
                    else:
                        w = open(missing_dest_file, "a+")
                        w.write(file_path + "\n")
                        w.close()
                else:
                    j = 1
                    while j <= files_amount_in_line:
                        file_name = file_path + "/" + str(j)
                        if os.path.isfile(file_name):
                            w = open(existing_dest_file, "a+")
                            w.write(file_name + "\n")
                            w.close()
                        else:
                            w = open(missing_dest_file, "a+")
                            w.write(file_name + "\n")
                            w.close()
                        j += 1
                progress(reading_progress, num_lines, status="Reading files from the original list")
                reading_progress += 1
                num_lines_with_multipage += files_amount_in_line
        f.close()
        print("\n" + "Total files in original file after counting multipage files : " + str(
            num_lines_with_multipage) + "\n")
    except Exception as ex:
        print('Exception in main:')
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
