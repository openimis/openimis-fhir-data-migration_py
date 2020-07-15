from fhir.resources.bundle import Bundle
from fhir.resources.claim import Claim
from fhir.resources.fhirabstractbase import FHIRValidationError
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import json
import requests
from requests.auth import HTTPBasicAuth
import os
import getpass

baseUrl = "http://localhost:8000/api_fhir_r4/"
pageOffset = "&page-offset="
pageCount = "?_count=200"
user = "spiderman"
password = "spiderman"
EOF = -1
folder_name = 'json_files'
PATH = 'C:/openIMIS/openIMIS-be/openimis-fhir-data-migration_py/' + folder_name
INPUT = ["Location", "Patient", "PractitionerRole", "Practitioner", "Claim", "ClaimResponse", "CommunicationRequest",
         "Medication", "Condition", "ActivityDefinition", "HealthcareService"]


def max_page_number(url):

    size = 1
    req = requests.get(url, auth=HTTPBasicAuth(user, password))
    next_site_url = url + pageOffset

    while req.status_code != 404:

        size += 1
        url = next_site_url + str(size)
        req = requests.get(url, auth=HTTPBasicAuth(user, password))
        continue

    return size - 1


def read_json(input):

    page = 1
    url = baseUrl + input + "/" + pageCount
    last_page = max_page_number(url)
    req = requests.get(url, auth=HTTPBasicAuth(user, password))
    data = json.loads(req.text)
    next_site_url = url + pageOffset

    if input == "Claim":
        data = data.get('entry')

    while page < last_page:

        page += 1
        url = next_site_url + str(page)
        req = requests.get(url, auth=HTTPBasicAuth(user, password))
        resources_next_page = json.loads(req.text)
        resources_next_page = resources_next_page.get('entry')

        for i in range(len(resources_next_page)):
            if input == "Claim":
                data.append(resources_next_page[i])
            else:
                data['entry'].insert(EOF, resources_next_page[i])

    return data


def create_location_table():

    query = ("""DROP TABLE IF EXISTS Location;
                CREATE TABLE Location (
                location_id INT PRIMARY KEY NOT NULL, 
                identifier VARCHAR(255) NOT NULL, 
                name VARCHAR(100),
                physical_type VARCHAR(3),
                part_of VARCHAR(255) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("Location")
    bundle = Bundle(data)
    id = 0

    for i in range(len(bundle.entry)):
        try:
            location = bundle.entry[i].resource
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Location (location_id, identifier, name, physical_type, part_of) 
                     VALUES (%s,%s,%s,%s,%s)""")

        id = id + 1
        identifier = location.id
        name = None
        physical_type = None
        part_of = None

        if location.name is not None:
            name = location.name
        if location.physicalType is not None:
            physical_type = location.physicalType.coding[0].code
        if location.partOf is not None:
            part_of = location.partOf.reference

        data_to_insert = (id, identifier, name, physical_type, part_of)
        cursor.execute(insert, data_to_insert)
        connection.commit()

    print("Location table created\n")


def create_patient_table():

    query = ("""DROP TABLE IF EXISTS Patient;
                CREATE TABLE Patient (
                patient_id INT PRIMARY KEY NOT NULL, 
                identifier VARCHAR(255) NOT NULL, 
                name VARCHAR(255),
                birth_date DATE,
                gender VARCHAR(50),
                marital_status VARCHAR(5),
                link_other VARCHAR(255),
                link_type VARCHAR(50),
                photo_creation DATE,
                photo_url VARCHAR(255),
                telecom_phone VARCHAR(100),
                telecom_email VARCHAR(100),
                general_practitioner VARCHAR(255), 
                address VARCHAR(100),
                extension_poverty_status VARCHAR(10),
                extension_is_head BOOLEAN,
                extension_registration_date DATE, 
                extension_location_code VARCHAR(255),
                extension_education_code VARCHAR(10),
                extension_profession_code VARCHAR(10) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("Patient")
    bundle = Bundle(data)
    id = 0

    for i in range(len(bundle.entry)):
        try:
            patient = bundle.entry[i].resource
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Patient (patient_id, identifier, name, birth_date, gender, marital_status, link_other,
                     link_type, photo_creation, photo_url, telecom_phone, telecom_email, general_practitioner, address, 
                     extension_poverty_status, extension_is_head, extension_registration_date, extension_location_code,
                     extension_education_code, extension_profession_code) 
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""")

        id = id + 1
        identifier = patient.id
        gender = patient.gender
        first_name = ""
        last_name = ""
        birth_date = None
        marital_status = None
        link_other = None
        link_type = None
        photo_creation = None
        photo_url = None
        phone = None
        email = None
        practitioner = None
        address = None
        poverty_status = None
        head = None
        registration = None
        location = None
        education = None
        profession = None

        if patient.name is not None:
            if f_name := [x for x in patient.name if x.family is not None]:
                first_name = f_name[0].family
            if l_name := [x for x in patient.name if x.given is not None]:
                last_name = l_name[0].given[0]
        if patient.birthDate is not None:
            birth_date = patient.birthDate.date
        if patient.maritalStatus is not None:
            marital_status = patient.maritalStatus.coding[0].code
        if patient.link is not None:
            link_other = patient.link[0].other.reference
            link_type = patient.link[0].type
        if patient.photo is not None:
            photo_creation = patient.photo[0].creation.date
            photo_url = patient.photo[0].url
        if patient.telecom is not None:
            if tel_p := [x for x in patient.telecom if x.system == "phone"]:
                phone = tel_p[0].value
            if tel_e := [x for x in patient.telecom if x.system == "email"]:
                email = tel_e[0].value
        if patient.generalPractitioner is not None:
            practitioner = patient.generalPractitioner[0].reference
        if patient.address is not None:
            if addr := [x for x in patient.address if x.type == "physical"]:
                address = addr[0].text
        if poverty := [x for x in patient.extension
                       if x.url == "https://openimis.atlassian.net/wiki/spaces/OP/pages/1556643849/povertyStatus"]:
            poverty_status = poverty[0].valueBoolean
        if is_head := [x for x in patient.extension
                       if x.url == "https://openimis.atlassian.net/wiki/spaces/OP/pages/960069653/isHead"]:
            head = is_head[0].valueBoolean
        if reg := [x for x in patient.extension
                   if x.url == "https://openimis.atlassian.net/wiki/spaces/OP/pages/960331779/registrationDate"]:
            registration = reg[0].valueDateTime.date
        if loc := [x for x in patient.extension
                   if x.url == "https://openimis.atlassian.net/wiki/spaces/OP/pages/960495619/locationCode"]:
            location = loc[0].valueReference.reference
        if edu := [x for x in patient.extension
                   if x.url == "https://openimis.atlassian.net/wiki/spaces/OP/pages/960331788/educationCode"]:
            education = edu[0].valueCoding.code
        if prof := [x for x in patient.extension
                    if x.url == "https://openimis.atlassian.net/wiki/spaces/OP/pages/960135203/professionCode"]:
            profession = prof[0].valueCoding.code

        data_to_insert = (id, identifier, first_name + " " + last_name, birth_date, gender, marital_status, link_other,
                          link_type, photo_creation, photo_url, phone, email, practitioner, address, poverty_status,
                          head, registration, location, education, profession)
        cursor.execute(insert, data_to_insert)
        connection.commit()

    print("Patient table created\n")


def create_practitioner_role_table():

    query = ("""DROP TABLE IF EXISTS Practitioner_Role;
                CREATE TABLE Practitioner_Role (
                practitioner_role_id INT PRIMARY KEY NOT NULL, 
                practitioner VARCHAR(255), 
                healthcare_service VARCHAR(255) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("PractitionerRole")
    bundle = Bundle(data)
    id = 0

    for i in range(len(bundle.entry)):
        try:
            practitoner_role = bundle.entry[i].resource
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Practitioner_Role (practitioner_role_id, practitioner, healthcare_service) 
                     VALUES (%s,%s,%s)""")

        id = id + 1
        practitioner = None
        hcs = None

        if practitoner_role.practitioner is not None:
            practitioner = practitoner_role.practitioner.reference
        if practitoner_role.healthcareService is not None:
            hcs = practitoner_role.healthcareService[0].reference

        data_to_insert = (id, practitioner, hcs)
        cursor.execute(insert, data_to_insert)
        connection.commit()

    print("PractitionerRole table created\n")


def create_practitioner_table():

    query = ("""DROP TABLE IF EXISTS Practitioner;
                CREATE TABLE Practitioner (
                practitioner_id INT PRIMARY KEY NOT NULL, 
                identifier VARCHAR(255) NOT NULL, 
                name VARCHAR(255),
                birth_date DATE,
                telecom_phone VARCHAR(100),
                telecom_email VARCHAR(100) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("Practitioner")
    bundle = Bundle(data)
    id = 0

    for i in range(len(bundle.entry)):
        try:
            practitioner = bundle.entry[i].resource
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Practitioner (practitioner_id, identifier, name, birth_date, 
                     telecom_phone, telecom_email) 
                     VALUES (%s,%s,%s,%s,%s,%s)""")

        id = id + 1
        identifier = practitioner.id
        first_name = ""
        last_name = ""
        birth_date = None
        phone = None
        email = None

        if practitioner.name is not None:
            if f_name := [x for x in practitioner.name if x.family is not None]:
                first_name = f_name[0].family
            if l_name := [x for x in practitioner.name if x.given is not None]:
                last_name = l_name[0].given[0]
        if practitioner.birthDate is not None:
            birth_date = practitioner.birthDate.date
        if practitioner.telecom is not None:
            if tel_p := [x for x in practitioner.telecom if x.system == "phone"]:
                phone = tel_p[0].value
            if tel_e := [x for x in practitioner.telecom if x.system == "email"]:
                email = tel_e[0].value

        data_to_insert = (id, identifier, first_name + " " + last_name, birth_date, phone, email)
        cursor.execute(insert, data_to_insert)
        connection.commit()

    print("Practitioner table created\n")


def create_claim_table():

    query = ("""DROP TABLE IF EXISTS Claim CASCADE;
                CREATE TABLE Claim (
                claim_id INT PRIMARY KEY NOT NULL, 
                identifier VARCHAR(255) NOT NULL, 
                created DATE NOT NULL,
                billable_period_start DATE,
                billable_period_end DATE, 
                type VARCHAR(10) NOT NULL, 
                total DECIMAL,
                supporting_info_guarantee_id VARCHAR(100),
                supporting_info_explanation VARCHAR(100),
                facility VARCHAR(255),
                patient VARCHAR(255) NOT NULL,
                enterer VARCHAR(255) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("Claim")
    id = 0

    for i in range(len(data)):
        try:
            claim = Claim(data[i]['resource'])
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Claim (claim_id, identifier, created, billable_period_start, billable_period_end, type, 
                     total, supporting_info_guarantee_id, supporting_info_explanation, facility, patient, enterer) 
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""")

        id = id + 1
        identifier = claim.id
        created = claim.created.date
        bp_start = None
        bp_end = None
        type = claim.type.text
        total = None
        guarantee_id = None
        explanation = None
        facility = None
        patient = claim.patient.reference
        enterer = None

        if claim.billablePeriod is not None:
            if claim.billablePeriod.end is not None:
                bp_end = claim.billablePeriod.end.date
            if claim.billablePeriod.start is not None:
                bp_start = claim.billablePeriod.start.date
        if claim.total is not None:
            total = claim.total.value
        if claim.supportingInfo is not None:
            if guar_id := [x for x in claim.supportingInfo if x.category.text == "guarantee_id"]:
                guarantee_id = guar_id[0].valueString
            if expl := [x for x in claim.supportingInfo if x.category.text == "explanation"]:
                explanation = expl[0].valueString
        if claim.facility is not None:
            facility = claim.facility.reference
        if claim.enterer is not None:
            enterer = claim.enterer.reference

        data_to_insert = (id, identifier, created, bp_start, bp_end, type, total, guarantee_id, explanation,
                          facility, patient, enterer)
        cursor.execute(insert, data_to_insert)
        connection.commit()

    print("Claim table created\n")


def create_claim_item_table():

    query = ("""DROP TABLE IF EXISTS Claim_Item;
                CREATE TABLE Claim_Item (
                claim_item_id INT PRIMARY KEY NOT NULL,
                claim_id INT NOT NULL,
                identifier VARCHAR(255), 
                category VARCHAR(25),
                product_or_service VARCHAR(20), 
                quantity DECIMAL,
                unit_price DECIMAL,
                extension_value_reference VARCHAR(255),
                FOREIGN KEY (claim_id) REFERENCES Claim (claim_id) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("Claim")
    item_id = 0
    claim_id = 0

    for i in range(len(data)):
        claim_id = claim_id + 1
        try:
            claim = Claim(data[i]['resource'])
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Claim_Item (claim_item_id, claim_id, identifier, category, product_or_service, 
                     quantity, unit_price, extension_value_reference) 
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""")

        for j in range(len(claim.item)):
            item_id = item_id + 1
            identifier = claim.id

            if claim.item is not None:
                category = claim.item[j].category.text
                pos = claim.item[j].productOrService.text
                quantity = claim.item[j].quantity.value
                price = claim.item[j].unitPrice.value
                extension = claim.item[j].extension[0].valueReference.reference

                data_to_insert = (item_id, claim_id, identifier, category, pos, quantity, price, extension)
                cursor.execute(insert, data_to_insert)
        connection.commit()

    print("ClaimItem table created\n")


def create_claim_diagnosis_table():

    query = ("""DROP TABLE IF EXISTS Claim_Diagnosis;
                CREATE TABLE Claim_Diagnosis (
                claim_diagnosis_id INT PRIMARY KEY NOT NULL,
                claim_id INT NOT NULL,
                identifier VARCHAR(255), 
                icd_code VARCHAR(25),
                sequence INTEGER NOT NULL, 
                diagnosis_reference VARCHAR(255),
                FOREIGN KEY (claim_id) REFERENCES Claim (claim_id) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("Claim")
    diagnosis_id = 0
    claim_id = 0

    for i in range(len(data)):
        claim_id = claim_id + 1
        try:
            claim = Claim(data[i]['resource'])
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Claim_Diagnosis (claim_diagnosis_id, claim_id, identifier, icd_code, sequence, 
                     diagnosis_reference) 
                     VALUES (%s,%s,%s,%s,%s,%s)""")

        for j in range(len(claim.diagnosis)):
            diagnosis_id = diagnosis_id + 1
            identifier = claim.id

            if claim.diagnosis is not None:
                icd = claim.diagnosis[j].type[0].coding[0].code
                sequence = claim.diagnosis[j].sequence
                reference = claim.diagnosis[j].diagnosisReference.reference

                data_to_insert = (diagnosis_id, claim_id, identifier, icd, sequence, reference)
                cursor.execute(insert, data_to_insert)
        connection.commit()

    print("ClaimDiagnosis table created\n")


def create_claim_insurance_table():

    query = ("""DROP TABLE IF EXISTS Claim_Insurance;
                CREATE TABLE Claim_Insurance (
                claim_insurance_id INT PRIMARY KEY NOT NULL,
                claim_id INT NOT NULL, 
                identifier VARCHAR(255), 
                coverage VARCHAR(255) NOT NULL,
                focal BOOLEAN NOT NULL, 
                sequence INT NOT NULL,
                FOREIGN KEY (claim_id) REFERENCES Claim (claim_id) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("Claim")
    insurance_id = 0
    claim_id = 0

    for i in range(len(data)):
        claim_id = claim_id + 1
        try:
            claim = Claim(data[i]['resource'])
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Claim_Insurance (claim_insurance_id, claim_id, identifier,  coverage, focal, sequence) 
                     VALUES (%s,%s,%s,%s,%s,%s)""")

        insurance_id = insurance_id + 1
        identifier = claim.id
        coverage = claim.insurance[0].coverage.reference
        focal = claim.insurance[0].focal
        sequence = claim.insurance[0].sequence

        data_to_insert = (insurance_id, claim_id, identifier, coverage, focal, sequence)
        cursor.execute(insert, data_to_insert)
        connection.commit()

    print("ClaimInsurance table created\n")


def create_claim_response_table():

    query = ("""DROP TABLE IF EXISTS Claim_Response CASCADE;
                CREATE TABLE Claim_Response (
                claim_response_id INT PRIMARY KEY NOT NULL, 
                identifier VARCHAR(255) NOT NULL, 
                created DATE NOT NULL, 
                outcome VARCHAR(25) NOT NULL,
                type VARCHAR(5) NOT NULL,
                status VARCHAR(50) NOT NULL,
                use VARCHAR(25) NOT NULL,
                insurer VARCHAR(255) NOT NULL, 
                patient VARCHAR(255) NOT NULL, 
                communication_request VARCHAR(255),
                request VARCHAR(255),
                requestor VARCHAR(255) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("ClaimResponse")
    bundle = Bundle(data)
    id = 0

    for i in range(len(bundle.entry)):
        try:
            claim_response = bundle.entry[i].resource
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Claim_Response (claim_response_id, identifier, created, outcome, type, status, use,
                     insurer, patient, communication_request, request, requestor) 
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""")

        id = id + 1
        identifier = claim_response.id
        created = claim_response.created.date
        outcome = claim_response.outcome
        type = claim_response.type.text
        status = claim_response.status
        use = claim_response.use
        insurer = claim_response.insurer.reference
        patient = claim_response.patient.reference
        comm_req = None
        request = None
        requestor = None

        if claim_response.communicationRequest is not None:
            comm_req = claim_response.communicationRequest[0].reference
        if claim_response.request is not None:
            request = claim_response.request.reference
        if claim_response.requestor is not None:
            requestor = claim_response.requestor.reference

        data_to_insert = (id, identifier, created, outcome, type, status, use, insurer, patient, comm_req, request,
                          requestor)
        cursor.execute(insert, data_to_insert)
        connection.commit()

    print("ClaimResponse table created\n")


def create_claim_response_total_table():

    query = ("""DROP TABLE IF EXISTS Claim_Response_Total;
                CREATE TABLE Claim_Response_Total (
                claim_response_total_id INT PRIMARY KEY NOT NULL,
                claim_response_id INT NOT NULL, 
                identifier VARCHAR(255), 
                category VARCHAR(50), 
                amount DECIMAL,
                FOREIGN KEY (claim_response_id) REFERENCES Claim_Response (claim_response_id) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("ClaimResponse")
    bundle = Bundle(data)
    response_id = 0
    total_id = 0

    for i in range(len(bundle.entry)):
        response_id = response_id + 1
        try:
            claim_response = bundle.entry[i].resource
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Claim_Response_Total (claim_response_total_id, claim_response_id, identifier, 
                     category, amount) 
                     VALUES (%s,%s,%s,%s,%s)""")

        if claim_response.total is not None:

            for j in range(len(claim_response.total)):
                total_id = total_id + 1
                identifier = claim_response.id
                category = claim_response.total[j].category.coding[0].code
                amount = claim_response.total[j].amount.value

                data_to_insert = (total_id, response_id, identifier, category, amount)
                cursor.execute(insert, data_to_insert)
        connection.commit()

    print("ClaimResponseTotal table created\n")


def create_claim_response_item_table():

    query = ("""DROP TABLE IF EXISTS Claim_Response_Item CASCADE;
                CREATE TABLE Claim_Response_Item (
                claim_response_item_id INT PRIMARY KEY NOT NULL, 
                claim_response_id INT NOT NULL, 
                identifier VARCHAR(255), 
                extension VARCHAR(255),
                FOREIGN KEY (claim_response_id) REFERENCES Claim_Response (claim_response_id) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("ClaimResponse")
    bundle = Bundle(data)
    item_id = 0
    response_id = 0

    for i in range(len(bundle.entry)):
        response_id = response_id + 1
        try:
            claim_response = bundle.entry[i].resource
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Claim_Response_Item (claim_response_item_id, claim_response_id, identifier, extension) 
                     VALUES (%s,%s,%s,%s)""")

        if claim_response.item is not None:

            for j in range(len(claim_response.item)):
                item_id = item_id + 1
                identifier = claim_response.id
                extension = claim_response.item[j].extension[0].valueReference.reference

                data_to_insert = (item_id, response_id, identifier, extension)
                cursor.execute(insert, data_to_insert)
        connection.commit()

    print("ClaimResponseItem table created\n")


def create_claim_response_item_adjudication_table():

    query = ("""DROP TABLE IF EXISTS Claim_Response_Item_Adjudication;
                CREATE TABLE Claim_Response_Item_Adjudication (
                claim_response_item_adjudication_id INT PRIMARY KEY NOT NULL,
                claim_response_item_id INT NOT NULL,
                identifier VARCHAR(255) NOT NULL, 
                extension VARCHAR(255), 
                category_code VARCHAR(5),
                category_text VARCHAR(50), 
                reason_code VARCHAR(5),
                reason_text VARCHAR(50),
                value DECIMAL, 
                amount DECIMAL,
                FOREIGN KEY (claim_response_item_id) REFERENCES Claim_Response_Item (claim_response_item_id) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("ClaimResponse")
    bundle = Bundle(data)
    adju_id = 0
    item_id = 0

    for i in range(len(bundle.entry)):
        try:
            claim_response = bundle.entry[i].resource
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Claim_Response_Item_Adjudication (claim_response_item_adjudication_id, 
                     claim_response_item_id, identifier, extension, category_code, category_text, 
                     reason_code, reason_text, value, amount) 
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""")

        if claim_response.item is not None:

            for j in range(len(claim_response.item)):
                item_id = item_id + 1
                identifier = claim_response.id

                for q in range(len(claim_response.item[j].adjudication)):
                    adju_id = adju_id + 1
                    extension = claim_response.item[j].extension[0].valueReference.reference
                    c_code = claim_response.item[j].adjudication[q].category.coding[0].code
                    c_text = claim_response.item[j].adjudication[q].category.text
                    r_code = None
                    r_text = None
                    value = None
                    price = None

                    if claim_response.item[j].adjudication[q].reason is not None:
                        r_code = claim_response.item[j].adjudication[q].reason.coding[0].code
                        r_text = claim_response.item[j].adjudication[q].reason.text
                    if claim_response.item[j].adjudication[q].value is not None:
                        value = claim_response.item[j].adjudication[q].value
                    if claim_response.item[j].adjudication[q].amount is not None:
                        price = claim_response.item[j].adjudication[q].amount.value

                    data_to_insert = (adju_id, item_id, identifier, extension, c_code, c_text, r_code,
                                      r_text, value, price)
                    cursor.execute(insert, data_to_insert)
        connection.commit()

    print("ClaimResponseItemAdjudication table created\n")


def create_communication_request_table():

    query = ("""DROP TABLE IF EXISTS Communication_Request;
                CREATE TABLE Communication_Request (
                communication_request_id INT PRIMARY KEY NOT NULL, 
                identifier VARCHAR(255) NOT NULL,
                status VARCHAR(25) NOT NULL, 
                care_rendered BOOLEAN,
                payment_asked BOOLEAN,
                drug_prescribed BOOLEAN,
                drug_received BOOLEAN,
                assessment VARCHAR(10) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("CommunicationRequest")
    bundle = Bundle(data)
    id = 0

    for i in range(len(bundle.entry)):
        id = id + 1
        try:
            communication_request = bundle.entry[i].resource
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Communication_Request (communication_request_id, identifier, status, care_rendered, 
                     payment_asked, drug_prescribed, drug_received, assessment) 
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""")

        identifier = communication_request.id
        status = communication_request.status
        care = None
        payment = None
        drug_pre = None
        drug_rec = None
        assessment = None

        if communication_request.reasonCode is not None:
            if cr := [x for x in communication_request.reasonCode if x.coding[0].code == "care_rendered"]:
                care = cr[0].text
            if pa := [x for x in communication_request.reasonCode if x.coding[0].code == "payment_asked"]:
                payment = pa[0].text
            if dp := [x for x in communication_request.reasonCode if x.coding[0].code == "drug_prescribed"]:
                drug_pre = dp[0].text
            if dr := [x for x in communication_request.reasonCode if x.coding[0].code == "drug_received"]:
                drug_rec = dr[0].text
            if assess := [x for x in communication_request.reasonCode if x.coding[0].code == "asessment"]:
                assessment = assess[0].text

        data_to_insert = (id, identifier, status, care, payment, drug_pre, drug_rec, assessment)
        cursor.execute(insert, data_to_insert)
        connection.commit()

    print("CommunicationRequest table created\n")


def create_medication_table():

    query = ("""DROP TABLE IF EXISTS Medication;
                CREATE TABLE Medication (
                medication_id INT PRIMARY KEY NOT NULL, 
                identifier VARCHAR(255) NOT NULL,
                code_coding VARCHAR(25), 
                code_text VARCHAR(255),
                form VARCHAR(255),
                extension_frequency INTEGER,
                extension_unit_price DECIMAL,
                extension_use_context_male VARCHAR(5),
                extension_use_context_female VARCHAR(5),
                extension_use_context_adult VARCHAR(5),
                extension_use_context_kid VARCHAR(5),
                extension_use_context_venue VARCHAR(5),
                extension_topic VARCHAR(5) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("Medication")
    bundle = Bundle(data)
    id = 0

    for i in range(len(bundle.entry)):
        id = id + 1
        try:
            medication = bundle.entry[i].resource
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Medication (medication_id, identifier, code_coding, code_text, form, 
                     extension_frequency, extension_unit_price, extension_use_context_male, extension_use_context_female, 
                     extension_use_context_adult, extension_use_context_kid, extension_use_context_venue, 
                     extension_topic) 
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""")

        identifier = medication.id
        c_coding = None
        c_text = None
        form = None
        frequency = None
        unit_price = None
        male = None
        female = None
        adult = None
        kid = None
        venue = None
        topic = None

        if medication.code is not None:
            c_coding = medication.code.coding[0].code
            c_text = medication.code.text
        if medication.form is not None:
            form = medication.form.text
        if medication.extension is not None:
            if freq := [x for x in medication.extension if x.url == "frequency"]:
                frequency = freq[0].valueInteger
            if price := [x for x in medication.extension if x.url == "unitPrice"]:
                unit_price = price[0].valueMoney.value
            if gender := [x for x in medication.extension if x.url == "useContextGender"]:
                if man := [x for x in gender[0].valueUsageContext.valueCodeableConcept.coding if x.display == "Male"]:
                    male = man[0].code
                if woman := [x for x in gender[0].valueUsageContext.valueCodeableConcept.coding if x.display == "Female"]:
                    female = woman[0].code
            if age := [x for x in medication.extension if x.url == "useContextAge"]:
                if adul := [x for x in age[0].valueUsageContext.valueCodeableConcept.coding if x.display == "Adult"]:
                    adult = adul[0].code
                if child := [x for x in age[0].valueUsageContext.valueCodeableConcept.coding if x.display == "Kid"]:
                    kid = child[0].code
            if ven := [x for x in medication.extension if x.url == "useContextVenue"]:
                venue = ven[0].valueUsageContext.valueCodeableConcept.coding[0].code
            if top := [x for x in medication.extension if x.url == "topic"]:
                topic = top[0].valueCodeableConcept.text

        data_to_insert = (id, identifier, c_coding, c_text, form, frequency, unit_price, male, female,
                          adult, kid, venue, topic)
        cursor.execute(insert, data_to_insert)
        connection.commit()

    print("Medication table created\n")


def create_activity_definition_table():

    query = ("""DROP TABLE IF EXISTS Activity_Definition;
                CREATE TABLE Activity_Definition (
                activity_definition_id INT PRIMARY KEY NOT NULL, 
                identifier VARCHAR(255) NOT NULL,
                status VARCHAR(25) NOT NULL , 
                date VARCHAR(50),
                name VARCHAR(25),
                title VARCHAR(255),
                topic VARCHAR(5),
                extension_frequency INTEGER,
                extension_unit_price DECIMAL,
                use_context_male VARCHAR(5),
                use_context_female VARCHAR(5),
                use_context_adult VARCHAR(5),
                use_context_kid VARCHAR(5),
                use_context_workflow VARCHAR(5), 
                use_context_venue VARCHAR(5) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("ActivityDefinition")
    bundle = Bundle(data)
    id = 0

    for i in range(len(bundle.entry)):
        id = id + 1
        try:
            activity_definition = bundle.entry[i].resource
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Activity_Definition (activity_definition_id, identifier, status, date, name, title, 
                     topic, extension_frequency, extension_unit_price, use_context_male, use_context_female, 
                     use_context_adult, use_context_kid, use_context_workflow, use_context_venue) 
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""")

        identifier = activity_definition.id
        status = activity_definition.status
        date = None
        name = None
        title = None
        topic = None
        frequency = None
        unit_price = None
        male = None
        female = None
        adult = None
        kid = None
        workflow = None
        venue = None

        if activity_definition.status is not None:
            status = activity_definition.status
        if activity_definition.date is not None:
            date = activity_definition.date.date
        if activity_definition.name is not None:
            name = activity_definition.name
        if activity_definition.title is not None:
            title = activity_definition.title
        if activity_definition.topic is not None:
            topic = activity_definition.topic[0].text
        if activity_definition.extension is not None:
            if freq := [x for x in activity_definition.extension if x.url == "frequency"]:
                frequency = freq[0].valueInteger
            if price := [x for x in activity_definition.extension if x.url == "unitPrice"]:
                unit_price = price[0].valueMoney.value
        if activity_definition.useContext is not None:
            if gender := [x for x in activity_definition.useContext if x.code.code == "useContextGender"]:
                if man := [x for x in gender[0].valueCodeableConcept.coding if x.display == "Male"]:
                    male = man[0].code
                if woman := [x for x in gender[0].valueCodeableConcept.coding if x.display == "Female"]:
                    female = woman[0].code
                if age := [x for x in activity_definition.useContext if x.code.code == "useContextAge"]:
                    if adul := [x for x in age[0].valueCodeableConcept.coding if x.display == "Adult"]:
                        adult = adul[0].code
                    if child := [x for x in age[0].valueCodeableConcept.coding if x.display == "Kid"]:
                        kid = child[0].code
                if ven := [x for x in activity_definition.useContext if x.code.code == "useContextVenue"]:
                    venue = ven[0].valueCodeableConcept.coding[0].code
                if work := [x for x in activity_definition.useContext if x.code.code == "useContextWorkflow"]:
                    workflow = work[0].valueCodeableConcept.coding[0].code

        data_to_insert = (id, identifier, status, date, name, title, topic, frequency, unit_price, male,
                          female, adult, kid, workflow, venue)
        cursor.execute(insert, data_to_insert)
        connection.commit()

    print("ActivityDefinition table created\n")


def create_condition_table():

    query = ("""DROP TABLE IF EXISTS Condition;
                CREATE TABLE Condition (
                condition_id INT PRIMARY KEY NOT NULL, 
                identifier VARCHAR(255) NOT NULL,
                code_coding VARCHAR(25) NOT NULL , 
                code_text VARCHAR(255),
                recorded_date VARCHAR(50) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("Condition")
    bundle = Bundle(data)
    id = 0

    for i in range(len(bundle.entry)):
        id = id + 1
        try:
            condition = bundle.entry[i].resource
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Condition (condition_id, identifier, code_coding, code_text, recorded_date) 
                     VALUES (%s,%s,%s,%s,%s)""")

        identifier = condition.id
        code = None
        text = None
        date = None

        if condition.code is not None:
            code = condition.code.coding[0].code
            text = condition.code.text
        if condition.recordedDate is not None:
            date = condition.recordedDate.date

        data_to_insert = (id, identifier, code, text, date)
        cursor.execute(insert, data_to_insert)
        connection.commit()

    print("Condition table created\n")


def create_healthcare_service_table():

    query = ("""DROP TABLE IF EXISTS Healthcare_Service CASCADE;
                CREATE TABLE Healthcare_Service (
                healthcare_service_id INT PRIMARY KEY NOT NULL, 
                identifier VARCHAR(255) NOT NULL, 
                name VARCHAR(255),
                category VARCHAR(5),
                type VARCHAR(5),
                speciality VARCHAR(5),
                location VARCHAR(255),
                extra_details VARCHAR(100),
                telecom_phone VARCHAR(100),
                telecom_email VARCHAR(100),
                program VARCHAR(5),
                coverage_area VARCHAR(255) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("HealthcareService")
    bundle = Bundle(data)
    id = 0

    for i in range(len(bundle.entry)):
        id = id + 1
        try:
            healthcare_service = bundle.entry[i].resource
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Healthcare_Service (healthcare_service_id, identifier, name, category, type, 
                     speciality, location, extra_details, telecom_phone, telecom_email, program, coverage_area) 
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""")

        identifier = healthcare_service.id
        name = None
        category = None
        type = None
        speciality = None  # has to be implmented, always None
        location = None
        details = None
        phone = None
        email = None
        program = None
        area = None

        if healthcare_service.name is not None:
            name = healthcare_service.name
        if healthcare_service.category is not None:
            category = healthcare_service.category[0].coding[0].code
        if healthcare_service.type is not None:
            type = healthcare_service.type[0].coding[0].code
        if healthcare_service.location is not None:
            location = healthcare_service.location[0].reference
        if healthcare_service.extraDetails is not None:
            details = healthcare_service.extraDetails
        if healthcare_service.telecom is not None:
            if tel_p := [x for x in healthcare_service.telecom if x.system == "phone"]:
                phone = tel_p[0].value
            if tel_e := [x for x in healthcare_service.telecom if x.system == "email"]:
                email = tel_e[0].value
        if healthcare_service.program is not None:
            program = healthcare_service.program[0].coding[0].code
        if healthcare_service.coverageArea is not None:
            area = healthcare_service.coverageArea[0].reference

        data_to_insert = (id, identifier, name, category, type, speciality, location, details, phone,
                          email, program, area)
        cursor.execute(insert, data_to_insert)
        connection.commit()

    print("HealthcareService table created\n")


def create_healthcare_service_coverage_area_table():

    query = ("""DROP TABLE IF EXISTS Healthcare_Service_Coverage_Area;
                CREATE TABLE Healthcare_Service_Coverage_Area (
                healthcare_service_coverage_area_id INT PRIMARY KEY NOT NULL,
                healthcare_service_id INT NOT NULL, 
                identifier VARCHAR(255), 
                coverage_area VARCHAR(255),
                FOREIGN KEY (healthcare_service_id) REFERENCES Healthcare_Service (healthcare_service_id) );""")

    cursor.execute(query)
    connection.commit()
    data = read_json("HealthcareService")
    bundle = Bundle(data)
    health_id = 0
    coverage_id = 0

    for i in range(len(bundle.entry)):
        health_id = health_id + 1
        try:
            healthcare_service = bundle.entry[i].resource
        except FHIRValidationError:
            continue

        insert = ("""INSERT INTO Healthcare_Service_Coverage_Area (healthcare_service_coverage_area_id, 
                     healthcare_service_id, identifier, coverage_area) 
                     VALUES (%s,%s,%s,%s)""")

        if healthcare_service.coverageArea is not None:

            for j in range(len(healthcare_service.coverageArea)):
                coverage_id = coverage_id + 1
                identifier = healthcare_service.id
                area = healthcare_service.coverageArea[j].reference

                data_to_insert = (coverage_id, health_id, identifier, area)
                cursor.execute(insert, data_to_insert)
        connection.commit()

    print("HealthcareServiceCoverageArea table created\n")


class Database:

    def create_tables(self):
        create_location_table()
        create_patient_table()
        create_practitioner_role_table()
        create_practitioner_table()
        create_claim_table()
        create_claim_item_table()
        create_claim_diagnosis_table()
        create_claim_insurance_table()
        create_claim_response_table()
        create_claim_response_total_table()
        create_claim_response_item_table()
        create_claim_response_item_adjudication_table()
        create_communication_request_table()
        create_medication_table()
        create_activity_definition_table()
        create_condition_table()
        create_healthcare_service_table()
        create_healthcare_service_coverage_area_table()

    tables = {1: create_activity_definition_table, 2: create_claim_table, 3: create_claim_item_table,
              4: create_claim_diagnosis_table, 5: create_claim_insurance_table, 6: create_claim_response_table,
              7: create_claim_response_total_table, 8: create_claim_response_item_table,
              9: create_claim_response_item_adjudication_table, 10: create_communication_request_table,
              11: create_condition_table, 12: create_healthcare_service_table,
              13: create_healthcare_service_coverage_area_table, 14: create_location_table, 15: create_medication_table,
              16: create_patient_table, 17: create_practitioner_table, 18: create_practitioner_role_table}


class Json:

    def write_json(self):

        path = PATH
        os.mkdir(path)

        for i in range(len(INPUT)):
            data = read_json(INPUT[i])
            with open(folder_name + '/' + str(INPUT[i]) + '.json', 'w') as outfile:
                json.dump(data, outfile, indent=4)
            print("Created " + INPUT[i] + ".json file (" + str(i + 1) + "/" + str(len(INPUT)) + ")\n")

        print("Completed all files")

    def remove_and_write_files(self):

        for i in range(len(INPUT)):
            os.remove(folder_name + '/' + INPUT[i] + '.json')
            print("Removed " + INPUT[i] + ".json file (" + str(i + 1) + "/" + str(len(INPUT)) + ")\n")

        os.removedirs(folder_name)
        print("\nRemoved " + folder_name + " folder\n")
        print("\n-----------------------------------\n")
        self.write_json()


if __name__ == '__main__':

    loop = True
    while loop:

        print("\nTo create database tables press <1>\n"
              "To create json files press <2>\n"
              "To exit press <3>")
        number = input("> Your choice: ")
        print("\n")

        if number == "1":
            database_name = input("> Database Name: ")
            db_username = input("> Username: ")
            db_password = getpass.getpass("> Password: ")
            db_host = input("> Host Address: ")
            db_port = input("> Port Number: ")

            database = Database()
            connection = psycopg2.connect(dbname=database_name, user=db_username, password=db_password,
                                          host=db_host, port=db_port)
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = connection.cursor()
            question = input("\nDo you want to create all tables? (y|n) ", )
            print("\n"
                  "----------------------------------------------\n")
            if question == "y":
                database.create_tables()
            if question == "n":
                print("Which table do you want to create?\n")
                print("- Activity_Definition: <1>\n"
                      "- Claim: <2>\n"
                      "- Claim_Response: <3>\n"
                      "- Communication_Request: <4>\n"
                      "- Condition: <5>\n"
                      "- Healthcare_Service: <6>\n"
                      "- Location: <7>\n"
                      "- Medication: <8>\n"
                      "- Patient: <9>\n"
                      "- Practitioner: <10>\n"
                      "- Practitioner_Role: <11>\n")
                table = input("> Your choice: ")
                print("\n----------------------------------------------")
                print("\n")
                if table == "1":
                    database.tables[1]()
                if table == "2":
                    database.tables[2]()
                    database.tables[3]()
                    database.tables[4]()
                    database.tables[5]()
                if table == "3":
                    database.tables[6]()
                    database.tables[7]()
                    database.tables[8]()
                    database.tables[9]()
                if table == "4":
                    database.tables[10]()
                if table == "5":
                    database.tables[11]()
                if table == "6":
                    database.tables[12]()
                    database.tables[13]()
                if table == "7":
                    database.tables[14]()
                if table == "8":
                    database.tables[15]()
                if table == "9":
                    database.tables[16]()
                if table == "10":
                    database.tables[17]()
                if table == "11":
                    database.tables[18]()
                cursor.close()
                connection.close()

        if number == "2":
            print("\n----------------------------------------------")
            file = Json()
            if os.path.isdir(PATH):
                file.remove_and_write_files()
            else:
                file.write_json()

        if number == "3":
            print("Good Bye")
            loop = False



