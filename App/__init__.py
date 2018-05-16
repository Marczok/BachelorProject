import time
import mysql.connector
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import json
from mysql.connector import errorcode
import csv
import credentials as cr
import statistics as stat
import locale


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
            "SELECT timestamp, battery_level from battery order by timestamp desc limit 1000"
        cursor_input.execute(query)
        counter = 0
        for (timestamp, battery_level) in cursor_input:
            if counter % 50 == 0:
                print(parsing_input.parse_time(timestamp), "\t", battery_level)
            counter = counter + 1

    @staticmethod
    def print_ambient_noise(cursor_input, parsing_input):
        query = \
            "SELECT timestamp,double_decibels FROM plugin_ambient_noise"
        cursor_input.execute(query)
        for (timestamp, double_decibels) in cursor_input:
            print("{} | {}".format(parsing.parse_time(timestamp), double_decibels))


def count_rounded_average_percents(a, b):
    return int(round(a / b * 100))


def safe_get(input_data, index):
    if index < len(input_data):
        return input_data[index]
    else:
        return 10000


def count_number_of_changes_by_one_two_and_three_wrong(input_data):
    one_phase = 0
    two_phases = 0
    three_phases = 0
    for i in range(0, len(input_data)):
        if i < len(input_data):
            a = safe_get(input_data, i)
            b = safe_get(input_data, i + 1)
            c = safe_get(input_data, i + 2)
            d = safe_get(input_data, i + 3)
            if a == c:
                one_phase = one_phase + 1
            elif b == d:
                two_phases = two_phases + 1
            else:
                three_phases = three_phases + 1
    return one_phase, two_phases, three_phases


def count_number_of_changes_by_one_two_and_three(input_data):
    one_phase = 0
    two_phases = 0
    three_phases = 0
    for i in range(0, len(input_data)):
        if i < len(input_data):
            a = safe_get(input_data, i)
            b = safe_get(input_data, i + 1)
            if abs(a - b) == 1:
                one_phase = one_phase + 1
            elif abs(a - b) == 2:
                two_phases = two_phases + 1
            else:
                three_phases = three_phases + 1
    return one_phase, two_phases, three_phases


def count_number_of_phases(input_data):
    deep = 0
    light = 0
    rem = 0
    awake = 0
    for i in input_data:
        if i == 1:
            deep = deep + 1
        elif i == 2:
            light = light + 1
        elif i == 3:
            rem = rem + 1
        else:
            awake = awake + 1
    return deep, light, rem, awake


def count_sleep_duration(input_data):
    duration_counter = 0
    for x in input_data:
        duration_counter = duration_counter + x
    return duration_counter


def transform_duration_to_minutes(input_data):
    duration_in_minutes_output = []
    for x in input_data:
        duration_in_minutes_output.append(x / 60)
    return duration_in_minutes_output


def minutes_to_hours(minutes):
    return round(minutes / 60, 2)


class DataProcessing:
    @staticmethod
    def create_lists_from_data(data_input):
        times_output = []
        levels_output = []
        levels_as_word_output = []
        duration_output = []
        for item in data_input:
            times_output.append(item["dateTime"][11:-7])
            levels_as_word_output.append(item["level"])
            levels_output.append(parsing.sleep_to_number(item["level"]))
            duration_output.append(item["seconds"])
        return times_output, levels_output, levels_as_word_output, duration_output

    @staticmethod
    def count_sleep_statistics(sleep_data_input, levels_input, duration_input, input_name):
        summary = sleep_data_input['levels']['summary']

        total_sleep_in_minutes = int(sleep_data_input['timeInBed'])
        total_sleep_in_hours = minutes_to_hours(total_sleep_in_minutes)

        deep_in_minutes = int(summary['deep']['minutes'])
        light_in_minutes = int(summary['light']['minutes'])
        rem_in_minutes = int(summary['rem']['minutes'])
        wake_in_minutes = int(sleep_data_input['minutesAwake'])
        deep_in_hours = minutes_to_hours(deep_in_minutes)
        light_in_hours = minutes_to_hours(light_in_minutes)
        rem_in_hours = minutes_to_hours(rem_in_minutes)
        wake_in_hours = minutes_to_hours(wake_in_minutes)

        deep_in_percents = count_rounded_average_percents(deep_in_minutes, total_sleep_in_minutes)
        light_in_percents = count_rounded_average_percents(light_in_minutes, total_sleep_in_minutes)
        rem_in_percents = count_rounded_average_percents(rem_in_minutes, total_sleep_in_minutes)
        wake_in_percents = count_rounded_average_percents(wake_in_minutes, total_sleep_in_minutes)

        phase_duration_average_in_hours = minutes_to_hours(total_sleep_in_minutes / len(levels_input))
        phase_duration_average_in_percents = \
            count_rounded_average_percents(phase_duration_average_in_hours, total_sleep_in_hours)

        duration_data_in_minutes = transform_duration_to_minutes(duration_input)
        deviation_in_minutes = round(stat.stdev(duration_data_in_minutes))
        deviation_in_hours = minutes_to_hours(deviation_in_minutes)
        deviation_in_percents = count_rounded_average_percents(deviation_in_minutes, total_sleep_in_minutes)

        (deep_count, light_count, rem_count, wake_count) = count_number_of_phases(levels_input)
        all_count = deep_count + light_count + rem_count + wake_count
        if all_count != len(levels_input):
            raise ValueError('Phases count is wrong counted')
        deep_count_per_hour = round(deep_count / total_sleep_in_hours, 2)
        light_count_per_hour = round(light_count / total_sleep_in_hours, 2)
        rem_count_per_hour = round(rem_count / total_sleep_in_hours, 2)
        wake_count_per_hour = round(wake_count / total_sleep_in_hours, 2)

        (one, two, three) = count_number_of_changes_by_one_two_and_three(levels_input)
        one_per_hour = round(one / total_sleep_in_hours, 2)
        two_per_hour = round(two / total_sleep_in_hours, 2)
        three_per_hour = round(three / total_sleep_in_hours, 2)

        return [
            input_name,
            total_sleep_in_hours,
            deep_in_hours,
            light_in_hours,
            rem_in_hours,
            wake_in_hours,
            deep_in_percents,
            light_in_percents,
            rem_in_percents,
            wake_in_percents,
            phase_duration_average_in_hours,
            phase_duration_average_in_percents,
            deviation_in_hours,
            deviation_in_percents,
            all_count,
            deep_count,
            light_count,
            rem_count,
            wake_count,
            deep_count_per_hour,
            light_count_per_hour,
            rem_count_per_hour,
            wake_count_per_hour,
            one,
            two,
            three,
            one_per_hour,
            two_per_hour,
            three_per_hour
        ]


if __name__ == '__main__':
    locale.setlocale(locale.LC_ALL, 'czech')
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
        database.print_battery_levels(cursor, parsing)
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
            "Total (h)",
            "Deep (h)",
            "Light (h)",
            "REM (h)",
            "Wake (h)",
            "Deep (%)",
            "Light (%)",
            "Rem (%)",
            "Wake (%)",
            "Average (h)",
            "Average (%)",
            "Deviation (h)",
            "Deviation (%)",
            "Total count",
            "Deep count",
            "Light count",
            "Rem count",
            "Wake count",
            "Deep count (1/h)",
            "Light count (1/h)",
            "Rem count (1/h)",
            "Wake count (1/h)",
            "Change by one",
            "Change by two",
            "Change by three",
            "Change by one (1/h)",
            "Change by two (1/h)",
            "Change by three (1/h)"
        ]]

        dates = []
        for sleep in sleeps:
            date = sleep[0]['dateOfSleep']
            if date in dates:
                continue
            dates.append(date)
            if date <= '2018-05-04':
                name = date + " Travnickova"
            elif date <= '2018-05-13':
                name = date + " Mikulcova"
            else:
                name = date + " Harthova"

            if date == '2018-05-04':
                data = sleep[1]['levels']['data'] + sleep[0]['levels']['data']
                # this sleep was divided into two parts for some reason
            else:
                data = sleep[0]['levels']['data']

            (times, levels, levels_as_word, duration) = data_processing.create_lists_from_data(data)
            if times and levels and levels_as_word and duration:
                csv_save.save_to_csv(times, levels, levels_as_word, duration, name)
                # chart_class.create_chart(levels, times, duration, name)

            if not date == '2018-05-04' and not date == '2018-05-15':
                statics = data_processing.count_sleep_statistics(sleep[0], levels, duration, name)
                statistics_data.append(statics)

        statistics_data = zip(*statistics_data)
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
