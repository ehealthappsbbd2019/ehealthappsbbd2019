import csv
from mimesis import Person, Address
from mimesis.enums import Gender
from datetime import date, timedelta
import random
from random import randrange


class LoadAndGenData:
    @staticmethod
    def get_insert_patient_queries():
        """
        read the dataset and creates insert statements
        :return: list of insert statements
        """
        with open("dataset/dataR2.csv") as csv_file:
            dataset = csv.reader(csv_file, delimiter=',')
            queries = []
            next(dataset, None)
            for data in dataset:
                random_date = LoadAndGenData._get_random_date()
                query = "INSERT INTO exam (id_doctor, id_lab_worker, id_patient, date, glucose," \
                        " insulin, leptin, adiponectin, resistin, MCP-1) VALUES ({}, {}," \
                        " {}, '{}', {}, {}, {}, {}, {}, {})".format(randrange(10), randrange(5), randrange(100), random_date, data[2], data[3], data[5], data[6], data[7], data[8])
                queries.append(query)
            return queries

    @staticmethod
    def _get_random_date():
        """
        generate a random date
        :return: a random date
        """
        start_dt = date.today().replace(day=1, month=1).toordinal()
        end_dt = date.today().toordinal()
        random_day = date.fromordinal(random.randint(start_dt, end_dt))
        return random_day

    @staticmethod
    def generate_random_doctors(number_of_doctors):
        """
        generate insert statements to doctors
        :param number_of_doctors: desired number of doctors
        :return: list of sql statements
        """
        person = Person('en')
        ad = Address('en')
        queries = []
        specialities = ["oncologist", "obstetrician", "gynecologist"]
        for id in range(number_of_doctors):
            if randrange(1) == 0:
                name = person.full_name(gender=Gender.FEMALE).replace("'", "")
            else:
                name = person.full_name(gender=Gender.MALE).replace("'", "")
            address = ad.address()
            date_birth = LoadAndGenData._get_random_date() - timedelta(days=365*(randrange(45)+25))
            speciality = specialities[randrange(len(specialities))]
            query = "INSERT INTO doctor (id, name, address, date_of_birth, specialty) VALUES ({}, '{}', '{}', '{}', '{}')".format(id, name, address, date_birth, speciality)
            queries.append(query)
        return queries

    @staticmethod
    def generate_random_patients(number_of_patients):
        """
        generate insert statements to patients
        :param number_of_patients: desired number of patients
        :return: list of sql statements
        """
        person = Person('en')
        ad = Address('en')
        queries = []
        for id in range(number_of_patients):
            name = person.full_name(gender=Gender.FEMALE).replace("'", "")
            gender = "F"
            address = ad.address()
            date_birth = LoadAndGenData._get_random_date() - timedelta(days=365*(randrange(45)+18))
            query = "INSERT INTO patient (id, name, address, date_of_birth, gender) VALUES ({}, '{}', '{}', '{}', '{}')"\
                .format(id, name, address, date_birth, gender)
            queries.append(query)
        return queries

    @staticmethod
    def generate_random_labworker(number_of_labworkers):
        """
        generate insert statements to laboratory workers
        :param number_of_labworkers: desired number of laboratory workers
        :return: list of sql statements
        """
        person = Person('en')
        ad = Address('en')
        queries = []
        professions = ["biomedic"]
        for id in range(number_of_labworkers):
            if randrange(1) == 0:
                name = person.full_name(gender=Gender.FEMALE).replace("'", "")
            else:
                name = person.full_name(gender=Gender.MALE).replace("'", "")
            address = ad.address()
            date_birth = LoadAndGenData._get_random_date() - timedelta(days=365*(randrange(45)+25))
            profession = professions[randrange(len(professions))]
            query = "INSERT INTO laboratory_worker (id, name, address, date_of_birth, occupation) VALUES ({}, '{}', '{}', '{}', '{}')"\
                .format(id, name, address, date_birth, profession)
            queries.append(query)
        return queries
