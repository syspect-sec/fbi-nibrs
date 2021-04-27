import csv

with open('/Users/development/Software/NIBRS/TMP/downloads/AL-1991/nibrs_bias_list.csv', 'r') as f:
    csv_reader = csv.reader(f, skipinitialspace=True)

    with open('/Users/development/Software/NIBRS/TMP/downloads/AL-1991/nibrs_bias_list.csv.test2', 'w') as f:
        csv_writer = csv.writer(f)
        for line in csv_reader:
            csv_writer.writerow(line)
