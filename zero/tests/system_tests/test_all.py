import subprocess


# This script assumes that zero is runnig and the
# location of the file system is FILE_SYSTEM_LOCATION

FILE_SYSTEM_LOCATION = "/home/kon/playground/mountpoint/"
TEST_FILE_1 = "wdTN0IE.png"
TEST_FILE_2 = "tiblis.jpg"
TEST_FOLDER_1 = "/home/kon/playground/Architecture/"


def test():
    commands = [
        f"cp {TEST_FOLDER_1}{TEST_FILE_1} {FILE_SYSTEM_LOCATION}",
        f"cp {TEST_FOLDER_1}{TEST_FILE_2} {FILE_SYSTEM_LOCATION}",
        f"cp {FILE_SYSTEM_LOCATION}{TEST_FILE_1} {FILE_SYSTEM_LOCATION}COPIED_{TEST_FILE_1}",
        f"mv {FILE_SYSTEM_LOCATION}{TEST_FILE_1} {FILE_SYSTEM_LOCATION}RENAMED_{TEST_FILE_1}",
        f"rm {FILE_SYSTEM_LOCATION}RENAMED_{TEST_FILE_1}",
        f"cp -r {TEST_FOLDER_1} {FILE_SYSTEM_LOCATION}",
        f"mv {FILE_SYSTEM_LOCATION}COPIED_{TEST_FILE_1} {FILE_SYSTEM_LOCATION}Architecture/",
        f"mv {FILE_SYSTEM_LOCATION}Architecture {FILE_SYSTEM_LOCATION}Architecture-RENAMED",
        f"ls {FILE_SYSTEM_LOCATION}",
        f"rm -rf {FILE_SYSTEM_LOCATION}*",
    ]
    counter = 0
    for command in commands:
        counter += 1
        print(counter)
        print(command)
        print(subprocess.check_output(command, shell=True))
