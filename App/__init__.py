import time
import mysql.connector
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import json
from mysql.connector import errorcode
import csv
import credentials as cr


class Parsing:
    @staticmethod
    def parse_data(data):
        try:
            parse = json.loads(data)
            return parse

        except (ValueError, KeyError, TypeError):
            print("JSON format error")

    @staticmethod
    def parse_time(epoch_time):
        return time.strftime('%d.%m.%Y %H:%M:%S', time.localtime(epoch_time / 1000))

    @staticmethod
    def parse_time_for_file_name(epoch_time):
        return time.strftime('%d.%m.%Y', time.localtime(epoch_time / 1000))

    @staticmethod
    def sleep_to_number(sleep_type):
        if sleep_type == "deep" or sleep_type == "asleep":
            return 1
        elif sleep_type == "light" or sleep_type == "restless":
            return 2
        elif sleep_type == "rem":
            return 3
        elif sleep_type == "wake" or sleep_type == "awake":
            return 4


class Chart:
    @staticmethod
    def create_chart(value, time, duration, chart_name):
        plot_series = pd.Series.from_array(value)
        chart = plot_series.plot(kind='bar', color='blue')
        chart.set_title(chart_name)
        chart.set_xlabel('Time')
        chart.set_ylabel('Level')
        chart.set_xticklabels(time)
        plt.yticks(np.arange(5), ["", "deep", "light", "rem", "wake"])
        plt.show()
        # plt.savefig()


class CSVSave:
    @staticmethod
    def save_to_csv(time_data, value_data, value_as_number, duration_data, name):
        data_table = [['time'] + time_data, ['level'] + value_data, ['level as number'] + value_as_number,
                      ['duration in seconds'] + duration_data]
        with open("../data {}.csv".format(name), 'w') as resultFile:
            wr = csv.writer(resultFile, dialect='excel', delimiter=';')
            wr.writerows(data_table)

    @staticmethod
    def save_statistics_to_csv(statistic_data_ipnut):
        with open("../statistics.csv", 'w') as resultFile:
            wr = csv.writer(resultFile, dialect='excel', delimiter=';')
            wr.writerows(statistic_data_ipnut)


class DatabaseWorks:
    @staticmethod
    def print_last_sync_info(cursor_input, parsing_input):
        sync_query = \
            "SELECT timestamp, device_id from aware_log order by timestamp desc limit 1"
        cursor_input.execute(sync_query)
        (timestamp, device_id) = cursor_input.next()

        sync_query = \
            "SELECT manufacturer from aware_device where device_id = \"{}\"".format(device_id)
        cursor_input.execute(sync_query)
        manufacturer = cursor_input.next()

        sync_query = \
            "SELECT battery_level from battery where device_id = \"{}\" order by timestamp desc limit 1".format(
                device_id)
        cursor_input.execute(sync_query)
        battery = cursor_input.next()

        print("Last sync {} from device {} with id {}, battery level {}.".format(
            parsing_input.parse_time(timestamp),
            manufacturer[0],
            device_id,
            battery[0]))

    @staticmethod
    def print_fitbit_sync_info(cursor_input, parsing_input):
        fitbit_query = \
            "SELECT timestamp, fitbit_battery, fitbit_last_sync FROM fitbit_devices ORDER BY timestamp desc limit 1"
        cursor_input.execute(fitbit_query)
        (timestamp, fitbit_battery, fitbit_last_sync) = cursor_input.next()
        print("Fitbit device last connected {} with battery status {}, last sync {}".format(
            parsing_input.parse_time(timestamp),
            fitbit_battery,
            fitbit_last_sync))

    @staticmethod
    def print_battery_levels(cursor_input, parsing_input):
        query = \
            "SELECT timestamp, battery_level from battery order by timestamp desc limit 500"
        cursor_input.execute(query)
        for (timestamp, battery_level) in cursor_input:
            print(parsing_input.parse_time(timestamp), "\t", battery_level)


def count_rounded_average_percents(a, b):
    return int(round(a / b * 100))


class DataProcessing:
    @staticmethod
    def create_lists_from_data(data_input):
        times = []
        levels = []
        levels_as_word = []
        duration = []
        for item in data_input:
            times.append(item["dateTime"][11:-7])
            levels_as_word.append(item["level"])
            levels.append(parsing.sleep_to_number(item["level"]))
            duration.append(item["seconds"])
        return times, levels, levels_as_word, duration

    @staticmethod
    def count_sleep_statistics(sleep_data_input):
        summary = sleep_data_input['levels']['summary']

        total_sleep_in_minutes = int(sleep_data_input['timeInBed'])

        sleep_in_minutes = int(sleep_data_input['minutesAsleep'])
        sleep_in_percents = count_rounded_average_percents(sleep_in_minutes, total_sleep_in_minutes)

        wake_in_minutes = int(sleep_data_input['minutesAwake'])
        wake_in_percents = count_rounded_average_percents(wake_in_minutes, total_sleep_in_minutes)

        deep_in_minutes = int(summary['deep']['minutes'])
        light_in_minutes = int(summary['light']['minutes'])
        rem_in_minutes = int(summary['rem']['minutes'])

        deep_in_percents = count_rounded_average_percents(deep_in_minutes, sleep_in_minutes)
        light_in_percents = count_rounded_average_percents(light_in_minutes, sleep_in_minutes)
        rem_in_percents = count_rounded_average_percents(rem_in_minutes, sleep_in_minutes)

        deep_count = int(summary['deep']['count'])
        light_count = int(summary['light']['count'])
        rem_count = int(summary['rem']['count'])
        wake_count = int(summary['wake']['count'])
        all_count = deep_count + light_count + rem_count + wake_count

        return [
            name,
            total_sleep_in_minutes,
            sleep_in_minutes,
            wake_in_minutes,
            sleep_in_percents,
            wake_in_percents,
            deep_in_minutes,
            light_in_minutes,
            rem_in_minutes,
            deep_in_percents,
            light_in_percents,
            rem_in_percents,
            all_count,
            deep_count,
            light_count,
            rem_count,
            wake_count
        ]


if __name__ == '__main__':
    try:
        cnx = mysql.connector.connect(user=cr.user(),
                                      database=cr.database(),
                                      host=cr.host(),
                                      password=cr.password())
        cursor = cnx.cursor()
        parsing = Parsing()
        chart_class = Chart()
        csv_save = CSVSave()
        database = DatabaseWorks()
        data_processing = DataProcessing()

        print("\n##################################################################################\n")
        database.print_last_sync_info(cursor, parsing)
        print("\n##################################################################################\n")
        database.print_fitbit_sync_info(cursor, parsing)
        print("\n##################################################################################\n")

        # Data processing
        query = "SELECT timestamp, fitbit_data FROM fitbit_data WHERE fitbit_data_type = \"sleep\" AND timestamp >= " \
                "1525154171019"

        sleeps = []
        cursor.execute(query)
        for (timestamp, fitbit_data) in cursor:
            parsed = parsing.parse_data(fitbit_data)
            some_dict = parsed[0]
            sleeps.append(some_dict["sleep"])

        statistics_data = [[
            "Date and name",
            "Total (min)",
            "Sleep (min)",
            "Wake (min)",
            "Sleep (%)",
            "Wake (%)",
            "Deep (min)",
            "Light (min)",
            "Rem (min)",
            "Deep average (%)",
            "Light average (%)",
            "Rem average (%)",
            "All count",
            "Deep count",
            "Light count",
            "Rem count",
            "Wake count",

        ]]

        for sleep in sleeps:
            date = sleep[0]['dateOfSleep']
            if date <= '2018-05-04':
                name = date + " Travnickova"
            else:
                name = date + " Mikulcova"

            if date == '2018-05-04':
                data = sleep[1]['levels']['data'] + sleep[0]['levels']['data']
                # this sleep was divided into two parts for some reason
            else:
                data = sleep[0]['levels']['data']

            (times, levels, levels_as_word, duration) = data_processing.create_lists_from_data(data)
            if times and levels and levels_as_word and duration:
                csv_save.save_to_csv(times, levels, levels_as_word, duration, name)
                chart_class.create_chart(levels, times, duration, name)

            if not date == '2018-05-04':
                statics = data_processing.count_sleep_statistics(sleep[0])
                statistics_data.append(statics)

        csv_save.save_statistics_to_csv(statistics_data)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cnx.close()
