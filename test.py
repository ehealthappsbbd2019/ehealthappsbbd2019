from queries_gen import LoadAndGenData
from storage_interface import StorageInterface

doctors = LoadAndGenData.generate_random_doctors(11)
patients = LoadAndGenData.generate_random_patients(101)
laboratory_workers = LoadAndGenData.generate_random_labworker(6)
exam = LoadAndGenData.get_insert_patient_queries()

print("RELATIONAL:\n")

print("\nINSERT DOCTORS:")
for i in doctors:
    r = StorageInterface.execute(i)
    print("SEND:", i)
    print("RECEIVED:", r)

print("\nINSERT PATIENTS:")
for i in patients:
    r = StorageInterface.execute(i)
    print("SEND:", i)
    print("RECEIVED:", r)

print("\nINSERT LABORATORY WORKERS:")
for i in laboratory_workers:
    r = StorageInterface.execute(i)
    print("SEND:", i)
    print("RECEIVED:", r)



print("\nSENDING DELETES:")
print("SEND:", "DELETE FROM doctor WHERE id=10")
r = StorageInterface.execute("DELETE FROM doctor WHERE id=10")
print("RECEIVED:", r)

print("SEND:", "DELETE FROM patient WHERE id=100")
r = StorageInterface.execute("DELETE FROM patient WHERE id=100")
print("RECEIVED:", r)

print("SEND:", "DELETE FROM laboratory_worker WHERE id=5")
r = StorageInterface.execute("DELETE FROM laboratory_worker WHERE id=5")
print("RECEIVED:", r)



print("\nSENDING UPDATES:")
print("SEND:", "UPDATE doctor SET name = 'test' WHERE id=1")
r = StorageInterface.execute("UPDATE doctor SET name = 'test' WHERE id=1")
print("RECEIVED:", r)

print("SEND:", "UPDATE patient SET name = 'test' WHERE id=1")
r = StorageInterface.execute("UPDATE patient SET name = 'test' WHERE id=1")
print("RECEIVED:", r)

print("SEND:", "UPDATE laboratory_worker SET name = 'test' WHERE id=1")
r = StorageInterface.execute("UPDATE laboratory_worker SET name = 'test' WHERE id=1")
print("RECEIVED:", r)


print("\nSENDING SELECTS:")
print("SEND:", "SELECT * FROM doctor")
r = StorageInterface.execute("SELECT * FROM doctor")
print("RECEIVED:", r)

print("SEND:", "SELECT * FROM patient")
r = StorageInterface.execute("SELECT * FROM patient")
print("RECEIVED:", r)

print("SEND:", "SELECT * FROM laboratory_worker")
r = StorageInterface.execute("SELECT * FROM laboratory_worker")
print("RECEIVED:", r)

print("\nBLOCKCHAIN:\n")

print("\nINSERT:")
for i in exam:
    r = StorageInterface.execute(i)
    print("SEND:", i)
    print("RECEIVED:", r)

print("\nUPDATE:")
print("UPDATE exam SET glucose = 2, insulin = 7, leptin = 21, adiponectin = 34, resistin = 24, MCP-1 = 1 WHERE id_doctor = 3 AND id_lab_worker = 2 AND id_patient = 99 AND date = '2019-02-12'")
r = StorageInterface.execute("UPDATE exam SET glucose = 2, insulin = 7, leptin = 21, adiponectin = 34, resistin = 24, MCP-1 = 1 WHERE id_doctor = 3 AND id_lab_worker = 2 AND id_patient = 99 AND date = '2019-02-12'")
print("RECEIVED:", r)

print("\nSELECT:")
print("SEND:", "SELECT * FROM exam")
r = StorageInterface.execute("SELECT * FROM exam")
print("RECEIVED:", r)

print("\nDELETE:")
print("SEND:", "DELETE FROM exam")
StorageInterface.execute("DELETE FROM exam")
