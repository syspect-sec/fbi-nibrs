with open("../primary_key_list.txt", "r") as infile:
    contents = infile.readlines()


with open("../primary_key_list_2.txt", "w") as outfile:
    for line in contents:
        line = line.lower().strip()
        outfile.write(line + "\n")
